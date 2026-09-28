// The ticket endpoint (ADR-0027): the site backend's minting half, for a
// client that must not hold a secret.
//
// Contract (agreed with the blog client, REQ-mp-ticket-endpoint):
//   POST <this worker>          same origin as the page; no CORS anywhere
//   body {"src": "https://<host>/<prefix>/<object>"}   the raw, unsigned URL
//   <- 200 {"url": "<presigned>", "expiresAt": "<ISO 8601>"}
//      Cache-Control: no-store; a session cookie is set on first success and
//      every later mint derives its `session` FROM that cookie — one cookie
//      must always mint with the same session id, because the origin charges
//      its budgets per session (a refresh that invents a new id hands the
//      viewer a fresh budget).
//   refusals are plain non-2xx (the client falls back to the bare URL);
//   refusals that carry detail teach an attacker more than they teach the site.
//
// Everything deployment-specific comes from bindings — no host, tenant or
// origin appears in this file. The secret is a wrangler secret (write-only for
// everyone but the account), so the blog side can ship this worker's route
// without ever holding it.
//
// Runs on both the Workers runtime and plain node >= 18 (global crypto,
// Request, Response), which is what the contract tests rely on.

const DEFAULT_EXPIRES = 3600; // 1 h: the client renews; a leak lives an hour
const DEFAULT_RATE_PER_MIN = 30;
const SESSION_COOKIE = "ticket_session";
const SESSION_MAX_AGE = 7 * 24 * 3600; // >= the 24 h the client asked for

const enc = new TextEncoder();

// The one encoder both sides must agree on: AWS unreserved characters pass,
// everything else is percent-encoded UTF-8, uppercase hex. Python's
// `quote(v, safe=...)` (presign.py) and this function produce identical bytes.
const UNRESERVED = /[A-Za-z0-9_.~-]/;

export function uriEncode(value, keepSlash) {
  let out = "";
  for (const ch of value) {
    if (UNRESERVED.test(ch) || (keepSlash && ch === "/")) {
      out += ch;
    } else {
      for (const byte of enc.encode(ch)) out += "%" + byte.toString(16).toUpperCase().padStart(2, "0");
    }
  }
  return out;
}

function hex(bytes) {
  return [...new Uint8Array(bytes)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function hmac(key, data) {
  const k = await crypto.subtle.importKey(
    "raw",
    typeof key === "string" ? enc.encode(key) : key,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  return new Uint8Array(await crypto.subtle.sign("HMAC", k, typeof data === "string" ? enc.encode(data) : data));
}

async function sha256Hex(data) {
  return hex(await crypto.subtle.digest("SHA-256", typeof data === "string" ? enc.encode(data) : data));
}

function canonicalQuery(pairs) {
  // Sort on the RAW pairs, then encode — the order the origin's verifier
  // applies, so neither side depends on AWS's tie-breaking corner cases.
  const ordered = [...pairs].sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : a[1] < b[1] ? -1 : 1));
  return ordered.map(([k, v]) => `${uriEncode(k, false)}=${uriEncode(v, false)}`).join("&");
}

// Mint one presigned GET. Byte-for-byte equivalent to
//   presign.py --host H --key K --session S --expires E --id I --secret SEC
// (--date fixed via `now`), which is asserted by tests/ticket_worker.rs.
export async function signUrl({ host, key, session, expires, accessKeyId, secret, region = "us-east-1", service = "s3", method = "GET", now = new Date() }) {
  // The reference signer normalizes a bare key to an absolute path, and the
  // canonical request is computed over that normalized form. The difference is
  // one `/` in the URL and one `/` inside the signature -- the test that
  // compares the two minters byte-for-byte is what catches it.
  const fullKey = key.startsWith("/") ? key : `/${key}`;
  const amzDate = now.toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
  const scopeDate = amzDate.slice(0, 8);
  const pairs = [
    ["X-Amz-Algorithm", "AWS4-HMAC-SHA256"],
    ["X-Amz-Credential", `${accessKeyId}/${scopeDate}/${region}/${service}/aws4_request`],
    ["X-Amz-Date", amzDate],
    ["X-Amz-Expires", String(expires)],
    ["X-Amz-SignedHeaders", "host"],
    ["session", session],
  ];
  const canonicalRequest = [
    method,
    uriEncode(fullKey, true),
    canonicalQuery(pairs),
    `host:${host}`,
    "",
    "host",
    "UNSIGNED-PAYLOAD",
  ].join("\n");
  const scope = `${scopeDate}/${region}/${service}/aws4_request`;
  const stringToSign = ["AWS4-HMAC-SHA256", amzDate, scope, await sha256Hex(canonicalRequest)].join("\n");
  let signingKey = await hmac(`AWS4${secret}`, scopeDate);
  signingKey = await hmac(signingKey, region);
  signingKey = await hmac(signingKey, service);
  signingKey = await hmac(signingKey, "aws4_request");
  const signature = hex(await hmac(signingKey, stringToSign));
  const query = canonicalQuery(pairs) + `&X-Amz-Signature=${signature}`;
  return `https://${host}${uriEncode(fullKey, true)}?${query}`;
}

export function originAllowed(request, allowedOrigin) {
  const origin = request.headers.get("origin");
  if (origin) return origin === allowedOrigin;
  const referer = request.headers.get("referer");
  return !!referer && referer.startsWith(`${allowedOrigin}/`);
}

// The raw embed is `https://<host>/<prefix>/<object>`; the mint signs the key.
// Two kinds of refusal, deliberately different codes: a malformed src
// (unparseable, a query riding along) is the client's mistake -> 400; a src
// that names another host or leaves the tenant prefix is authorization -> 403.
export function validateSrc(raw, host, prefix) {
  if (typeof raw !== "string" || raw === "") return { status: 400 };
  let url;
  try {
    url = new URL(raw);
  } catch {
    return { status: 400 };
  }
  if (url.protocol !== "https:" || url.search) return { status: 400 };
  if (url.host !== host) return { status: 403 };
  let key;
  try {
    key = decodeURIComponent(url.pathname.slice(1));
  } catch {
    return { status: 400 };
  }
  if (!key.startsWith(prefix)) return { status: 403 };
  return { key };
}

function sessionFromCookie(request, name) {
  const cookie = request.headers.get("cookie") ?? "";
  for (const part of cookie.split(";")) {
    const [k, ...rest] = part.trim().split("=");
    if (k === name && rest.length) return rest.join("=");
  }
  return null;
}

// A session's own budget is tiny (one mint per embed, one per hour after
// that), so the limiter only has to stop a runaway: per-session tokens in
// this isolate's memory. Workers isolate state is best-effort by design —
// that is fine for a valve that exists so one client cannot spend the
// origin's patience.
function makeLimiter(perMinute) {
  const seen = new Map();
  return (session, nowMs) => {
    const window = Math.floor(nowMs / 60000);
    const key = `${session}:${window}`;
    const used = (seen.get(key) ?? 0) + 1;
    seen.set(key, used);
    if (seen.size > 4096) for (const k of seen.keys()) if (!k.endsWith(`:${window}`)) seen.delete(k);
    return used <= perMinute;
  };
}

// One limiter per isolate: the valve only works if it remembers between
// requests (a limiter built per request counts every session at one).
let isolateLimiter = null;
function getLimiter(perMinute) {
  isolateLimiter ??= makeLimiter(perMinute);
  return isolateLimiter;
}

export async function handleTicketRequest(request, env, deps = {}) {
  const now = deps.now ? deps.now() : new Date();
  const nowMs = now.getTime();
  const respond = (status, body, extra = {}) =>
    new Response(body, {
      status,
      headers: { "cache-control": "no-store", "content-type": "application/json; charset=utf-8", ...extra },
    });

  if (request.method !== "POST") return respond(405, "{}");
  const required = ["TICKET_HOST", "TICKET_ID", "TICKET_PREFIX", "TICKET_SECRET", "ALLOWED_ORIGIN"];
  if (required.some((k) => !env[k])) {
    console.error("ticket endpoint misconfigured: missing", required.filter((k) => !env[k]).join(", "));
    return respond(500, "{}");
  }
  if (!originAllowed(request, env.ALLOWED_ORIGIN)) return respond(403, "{}");

  let src;
  try {
    src = JSON.parse(await request.text()).src;
  } catch {
    return respond(400, "{}");
  }
  const parsed = validateSrc(src, env.TICKET_HOST, env.TICKET_PREFIX);
  if (parsed.status) return respond(parsed.status, "{}");
  const key = parsed.key;

  const cookie = env.TICKET_SESSION_COOKIE ?? SESSION_COOKIE;
  const session = sessionFromCookie(request, cookie) ?? (deps.newSession ?? crypto.randomUUID.bind(crypto))();
  const perMinute = Number(env.TICKET_RATE_PER_MIN ?? DEFAULT_RATE_PER_MIN);
  const limiter = deps.limiter ?? getLimiter(perMinute);
  if (!limiter(session, nowMs)) return respond(429, "{}");

  const expires = Number(env.TICKET_EXPIRES_SECS ?? DEFAULT_EXPIRES);
  const url = await signUrl({
    host: env.TICKET_HOST,
    key,
    session,
    expires,
    accessKeyId: env.TICKET_ID,
    secret: env.TICKET_SECRET,
    now,
  });
  const headers = {};
  if (!sessionFromCookie(request, cookie)) {
    headers["set-cookie"] = `${cookie}=${session}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_MAX_AGE}`;
  }
  return respond(
    200,
    JSON.stringify({ url, expiresAt: new Date(nowMs + expires * 1000).toISOString().replace(/\.\d+Z$/, "Z") }),
    headers,
  );
}

export default {
  fetch: (request, env) => handleTicketRequest(request, env),
};
