// Why does a media element's open-ended range get headers and nothing else?
//
// Three arms in one browser session, so the connection pool, the TLS session
// and the origin are shared:
//   A. a <video> element reading the object (the failing shape),
//   B. a page-level `fetch()` with the SAME `Range: bytes=0-` (same origin,
//      same connection pool, but not the media stack),
//   C. curl, for the reference the other two are compared against.
//
// And the wire, from CDP, with the parts the player probe does not keep:
//   - `requestWillBeSentExtraInfo` / `responseReceivedExtraInfo`: the COMPLETE
//     header sets as actually sent and received;
//   - `loadingFailed`: the reason a request ended badly (canceled? net error?
//     blocked?), which is the difference between "the renderer gave up" and
//     "the network refused";
//   - `dataReceived`: whether ANY body byte arrived, per request;
//   - connection id and remote IP per response, so a per-POP or per-connection
//     story can be told apart from a per-request one.
//
// Usage: node media-wire-dump.mjs --target <base-url> --object <path>
//          [--page <path>] [--secs N] [--fetch-secs N]
// Env: CHROME=, PW=, PROXY=socks5://…, HOST_MAP="MAP host addr" (as the probe).
import { join } from 'node:path';

const args = process.argv.slice(2);
const arg = (n, d) => { const i = args.indexOf(`--${n}`); return i >= 0 && args[i + 1] ? args[i + 1] : d; };
const target = arg('target', 'https://cdn-oracle.isui.ren/googledrive1');
const objectPath = arg('object', 'demo.bin');
const pagePath = arg('page', 'test-page.html');
const mediaSecs = Number(arg('secs', '30'));
const fetchSecs = Number(arg('fetch-secs', '20'));
const chrome = process.env.CHROME || '/usr/bin/chromium';
// PW first (documented), then PW_DIR — the name run-lab.sh exports.
const pwDir = process.env.PW || process.env.PW_DIR || '/home/archivalera/.npm/_npx/9833c18b2d85bc59/node_modules/playwright-core';
const { chromium } = await import(join(pwDir, 'index.mjs')).catch(async () => await import(pwDir));

const base = target.replace(/\/$/, '');
const objectUrl = `${base}/${objectPath}`;
const filmPath = new URL(objectUrl).pathname;

const proxy = process.env.PROXY || '';
const hostMap = process.env.HOST_MAP || '';
const launchArgs = proxy ? [`--proxy-server=${proxy}`] : ['--no-proxy-server'];
if (hostMap) launchArgs.push(`--host-resolver-rules=${hostMap}`);
// EXTRA_ARGS lets a run change one browser behaviour (e.g. --disable-http2)
// without touching this file.
launchArgs.push(...(process.env.EXTRA_ARGS || '').split(/\s+/).filter(Boolean));

const browser = await chromium.launch({ executablePath: chrome, args: launchArgs });
const ctx = await browser.newContext();
const page = await ctx.newPage();
const cdp = await ctx.newCDPSession(page);
await cdp.send('Network.enable');

const recs = new Map();
const rec = (id) => { if (!recs.has(id)) recs.set(id, {}); return recs.get(id); };
const isFilm = (u) => u.includes(filmPath);

cdp.on('Network.requestWillBeSent', (e) => {
  if (!isFilm(e.request.url)) return;
  Object.assign(rec(e.requestId), { t0: Date.now(), url: e.request.url, initiator: e.initiator?.type });
});
cdp.on('Network.requestWillBeSentExtraInfo', (e) => {
  const r = recs.get(e.requestId); if (!r) return;
  r.reqHeaders = e.headers;
});
cdp.on('Network.responseReceived', (e) => {
  const r = recs.get(e.requestId); if (!r) return;
  Object.assign(r, {
    status: e.response.status, proto: e.response.protocol,
    remoteIP: e.response.remoteIPAddress, conn: e.response.connectionId,
    fromDisk: e.response.fromDiskCache, mime: e.response.mimeType,
  });
});
cdp.on('Network.responseReceivedExtraInfo', (e) => {
  const r = recs.get(e.requestId); if (!r) return;
  r.status2 = e.statusCode;
  r.respHeaders = e.headers;
});
cdp.on('Network.dataReceived', (e) => {
  const r = recs.get(e.requestId); if (!r) return;
  r.data = (r.data || 0) + (e.encodedDataLength || 0);
  r.dataEvents = (r.dataEvents || 0) + 1;
});
cdp.on('Network.loadingFinished', (e) => {
  const r = recs.get(e.requestId); if (!r) return;
  r.encoded = e.encodedDataLength; r.done = 'finished';
});
cdp.on('Network.loadingFailed', (e) => {
  const r = recs.get(e.requestId); if (!r) return;
  r.done = 'FAILED';
  r.fail = { errorText: e.errorText, canceled: e.canceled, blocked: e.blockedReason, type: e.type };
});

console.log(`object: ${objectUrl}`);
await page.goto(`${base}/${pagePath}`, { waitUntil: 'domcontentloaded' });
console.log(`vantage: ${proxy ? `via ${proxy} ${hostMap}` : 'direct'}`);

// --- Arm B: the same range, from a page-level fetch -------------------------
const b = await page.evaluate(async ({ url, secs }) => {
  const out = { status: null, headers: null, bytes: 0, events: 0, firstByteMs: null, done: false, err: null };
  const t0 = performance.now();
  try {
    const res = await fetch(url, { headers: { Range: 'bytes=0-' } });
    out.status = res.status;
    out.headers = Object.fromEntries([...res.headers.entries()]);
    const reader = res.body.getReader();
    const deadline = t0 + secs * 1000;
    while (performance.now() < deadline) {
      const { done, value } = await reader.read();
      if (done) { out.done = true; break; }
      out.events++;
      if (out.firstByteMs === null) out.firstByteMs = Math.round(performance.now() - t0);
      out.bytes += value.byteLength;
    }
    try { await reader.cancel(); } catch {}
  } catch (e) { out.err = String(e); }
  out.ms = Math.round(performance.now() - t0);
  return out;
}, { url: objectUrl, secs: fetchSecs });
console.log(`\nB) page fetch, Range: bytes=0-, ${fetchSecs}s window`);
console.log(`   status=${b.status} bytes=${b.bytes} chunks=${b.events} firstByteMs=${b.firstByteMs} done=${b.done} err=${b.err} ms=${b.ms}`);
if (b.headers) for (const [k, v] of Object.entries(b.headers)) console.log(`   < ${k}: ${v}`);

// --- Arm B2: a BOUNDED range, the shape the shard harness uses --------------
const b2 = await page.evaluate(async ({ url, secs }) => {
  const out = { status: null, bytes: 0, events: 0, firstByteMs: null, done: false, err: null };
  const t0 = performance.now();
  try {
    const res = await fetch(url, { headers: { Range: 'bytes=0-5242879' } });
    out.status = res.status;
    const reader = res.body.getReader();
    const deadline = t0 + secs * 1000;
    while (performance.now() < deadline) {
      const { done, value } = await reader.read();
      if (done) { out.done = true; break; }
      out.events++;
      if (out.firstByteMs === null) out.firstByteMs = Math.round(performance.now() - t0);
      out.bytes += value.byteLength;
    }
    try { await reader.cancel(); } catch {}
  } catch (e) { out.err = String(e); }
  out.ms = Math.round(performance.now() - t0);
  return out;
}, { url: objectUrl, secs: fetchSecs });
console.log(`\nB2) page fetch, Range: bytes=0-5242879 (5 MiB, bounded), ${fetchSecs}s window`);
console.log(`   status=${b2.status} bytes=${b2.bytes} chunks=${b2.events} firstByteMs=${b2.firstByteMs} done=${b2.done} err=${b2.err} ms=${b2.ms}`);

// --- Arm B3: are the OPEN-ENDED bytes the bytes that were asked for? --------
// Same offset, two shapes: `bytes=N-` (what the media stack sends) and a
// bounded range (the shape whose checksums were verified by the shard harness).
// If the first 64 KiB differ, the media element is right to reject them.
const b3 = await page.evaluate(async ({ url }) => {
  const read = async (range, n) => {
    const res = await fetch(url, { headers: { Range: range } });
    const reader = res.body.getReader();
    const buf = new Uint8Array(n);
    let off = 0;
    const deadline = performance.now() + 15000;
    while (off < n && performance.now() < deadline) {
      const { done, value } = await reader.read();
      if (done) break;
      const take = Math.min(value.byteLength, n - off);
      buf.set(value.subarray(0, take), off);
      off += take;
    }
    try { await reader.cancel(); } catch {}
    const sum = await crypto.subtle.digest('SHA-256', buf.subarray(0, off));
    const hex = (a) => [...a].map((b) => b.toString(16).padStart(2, '0')).join('');
    return {
      status: res.status, got: off,
      head: hex(buf.subarray(0, 16)),
      sha: hex(new Uint8Array(sum)).slice(0, 16),
    };
  };
  const open = await read('bytes=1572864-', 65536);
  const bounded = await read('bytes=1572864-1638399', 65536);
  return { open, bounded, same: open.sha === bounded.sha && open.got === 65536 };
}, { url: objectUrl });
console.log(`\nB3) same offset, open-ended vs bounded, first 64 KiB`);
console.log(`   open    : status=${b3.open.status} got=${b3.open.got} head=${b3.open.head} sha16=${b3.open.sha}`);
console.log(`   bounded : status=${b3.bounded.status} got=${b3.bounded.got} head=${b3.bounded.head} sha16=${b3.bounded.sha}`);
console.log(`   identical: ${b3.same}`);

// --- Arm A: the media element ----------------------------------------------
const a = await page.evaluate(async ({ url, secs }) => {
  const v = document.createElement('video');
  v.src = url; v.muted = true; v.autoplay = true;
  document.body.appendChild(v);
  const t0 = performance.now();
  await new Promise((r) => setTimeout(r, secs * 1000));
  return {
    ready: v.readyState, t: v.currentTime, dur: Number.isFinite(v.duration) ? v.duration : null,
    err: v.error ? `${v.error.code}/${v.error.message}` : null, ms: Math.round(performance.now() - t0),
  };
}, { url: objectUrl, secs: mediaSecs });
console.log(`\nA) media element, ${mediaSecs}s window`);
console.log(`   ready=${a.ready} currentTime=${a.t} duration=${a.dur} error=${a.err}`);

// --- The wire ---------------------------------------------------------------
await new Promise((r) => setTimeout(r, 1500));
const rows = [...recs.entries()].sort((x, y) => (x[1].t0 || 0) - (y[1].t0 || 0));
console.log(`\nwire: ${rows.length} request(s) on the object`);
const t0 = rows.length ? rows[0][1].t0 : Date.now();
const short = (h, n = 60) => (h == null ? '-' : String(h).slice(0, n));
for (const [id, r] of rows) {
  const at = `+${String((r.t0 || 0) - t0).padStart(6)}ms`;
  const range = r.reqHeaders ? (r.reqHeaders['Range'] || r.reqHeaders['range'] || '-') : '-';
  const cr = r.respHeaders ? (r.respHeaders['content-range'] || r.respHeaders['Content-Range'] || '-') : '-';
  const cl = r.respHeaders ? (r.respHeaders['content-length'] || '-') : '-';
  const edge = r.respHeaders ? (r.respHeaders['eo-cache-status'] || '-') : '-';
  const end = r.done === 'finished'
    ? `finished(${r.encoded ?? r.data ?? 0}B, dataEvents=${r.dataEvents ?? 0})`
    : `FAILED(${r.fail?.errorText ?? '?'} canceled=${r.fail?.canceled} blocked=${r.fail?.blocked ?? '-'} dataEvents=${r.dataEvents ?? 0})`;
  console.log(`  ${at} id=${short(id, 8)} ${r.status ?? r.status2 ?? '?'} ${r.proto ?? '-'} conn=${r.conn ?? '-'} ip=${r.remoteIP ?? '-'} range=${short(range, 22)} cr=${short(cr, 30)} cl=${cl} edge=${edge} body=${r.data ?? 0}B ${end}`);
}

// The complete header sets of the first request, both directions.
if (rows.length) {
  const [id, r] = rows[0];
  console.log(`\nfirst request (id=${id}) — headers actually sent:`);
  if (r.reqHeaders) for (const [k, v] of Object.entries(r.reqHeaders)) console.log(`   > ${k}: ${v}`);
  console.log(`first request — headers actually received:`);
  if (r.respHeaders) for (const [k, v] of Object.entries(r.respHeaders)) console.log(`   < ${k}: ${v}`);
}

const finished = rows.filter(([, r]) => r.done === 'finished').length;
const failed = rows.filter(([, r]) => r.done === 'FAILED').length;
const withData = rows.filter(([, r]) => (r.data || 0) > 0).length;
console.log(`\nsummary: ${rows.length} requests, ${finished} finished, ${failed} failed, ${withData} carried body bytes`);
await browser.close();
