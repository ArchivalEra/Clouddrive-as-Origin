// The CDN wire truth for a real player, request by request.
//
// `player-probe.mjs` reports what the player DID (the ranges it chose, the gaps
// the viewer felt). This one reports what the wire DID to it: for every media
// request, the bytes the browser actually received and the reason the request
// ended. The distinction matters because a media element that is handed 300
// bytes and EOF looks exactly like a player searching for metadata — and the
// fix for "the edge truncates" is not the fix for "the player gives up".
//
// CDP's errorText separates the two cases: `net::ERR_CONTENT_LENGTH_MISMATCH`
// is the server ending the body early, `net::ERR_ABORTED` is the browser
// walking away.
//
// Usage:
//   node cdn-wire-probe.mjs --base <url-base> --object <name> [--page <name>]
//                           [--secs <n>] [--window <bytes>]
//   `--window` > 0 rewrites the FIRST request of every connection to a bounded
//   Range (`bytes=N-(N+window-1)`) before it leaves the browser — the shape
//   option "answer an open-ended range with a bounded window" tested from the
//   player's side, with no origin change. 0 (default) rewrites nothing.
//   Env: CHROME=/usr/bin/chromium  PW=<playwright-core dir>
import { join } from 'node:path';

const args = process.argv.slice(2);
const arg = (name, def) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 && args[i + 1] ? args[i + 1] : def;
};
const base = arg('base', 'https://cdn-oracle.isui.ren/googledrive1').replace(/\/$/, '');
const object = arg('object', 'round3.mp4');
const pagePath = arg('page', 'test-page.html');
const secs = Number(arg('secs', '20'));
const windowBytes = Number(arg('window', '0'));
const chrome = process.env.CHROME || '/usr/bin/chromium';
// PW first (documented), then PW_DIR — the name run-lab.sh exports.
const pwDir = process.env.PW || process.env.PW_DIR || '/home/archivalera/.npm/_npx/9833c18b2d85bc59/node_modules/playwright-core';

const { chromium } = await import(join(pwDir, 'index.mjs')).catch(async () => await import(pwDir));
const url = `${base}/${object}`;

const browser = await chromium.launch({ executablePath: chrome, args: ['--no-proxy-server'] });
const ctx = await browser.newContext();
const page = await ctx.newPage();
const cdp = await ctx.newCDPSession(page);
await cdp.send('Network.enable');

const reqs = new Map();
const onObject = (u) => u.startsWith(url);
// Header names arrive in whatever case the protocol kept (h2 lowercases, CDP
// does not always), so every lookup goes through this.
const hdr = (h, name) => {
  for (const k of Object.keys(h ?? {})) if (k.toLowerCase() === name) return h[k];
  return undefined;
};
cdp.on('Network.requestWillBeSent', (e) => {
  if (!onObject(e.request.url)) return;
  reqs.set(e.requestId, { range: hdr(e.request.headers, 'range') ?? '(none)', t: Date.now(), bytes: 0 });
});
cdp.on('Network.responseReceived', (e) => {
  const r = reqs.get(e.requestId);
  if (!r) return;
  r.status = e.response.status;
  r.contentRange = hdr(e.response.headers, 'content-range') ?? '-';
  r.contentLength = hdr(e.response.headers, 'content-length') ?? '-';
  r.transfer = hdr(e.response.headers, 'transfer-encoding') ?? '-';
  r.edge = hdr(e.response.headers, 'eo-cache-status') ?? '-';
});
cdp.on('Network.dataReceived', (e) => {
  const r = reqs.get(e.requestId);
  if (r) r.bytes += e.dataLength;
});
cdp.on('Network.loadingFinished', (e) => {
  const r = reqs.get(e.requestId);
  if (r) r.end = `finished after ${Date.now() - r.t} ms`;
});
cdp.on('Network.loadingFailed', (e) => {
  const r = reqs.get(e.requestId);
  if (r) r.end = `FAILED ${e.errorText}${e.canceled ? ' (canceled)' : ''} after ${Date.now() - r.t} ms`;
});

if (windowBytes > 0) {
  // Rewrite the first Range of every request that arrives open-ended. The
  // player keeps asking for what it asked for; only the answer changes shape.
  const rewritten = new Set();
  await cdp.send('Fetch.enable', { patterns: [{ urlPattern: '*', requestStage: 'Request' }] });
  cdp.on('Fetch.requestPaused', async (e) => {
    const r = hdr(e.request.headers, 'range');
    const openEnded = r && /^bytes=\d+-$/.test(r);
    if (!openEnded) return void (await cdp.send('Fetch.continueRequest', { requestId: e.requestId }));
    const off = Number(r.slice(6, -1));
    const bounded = `bytes=${off}-${off + windowBytes - 1}`;
    if (!rewritten.has(off)) {
      rewritten.add(off);
      console.log(`  [rewrite] ${r} -> ${bounded}`);
    }
    await cdp.send('Fetch.continueRequest', { requestId: e.requestId, headers: [{ name: 'Range', value: bounded }] });
  });
}

await page.goto(`${base}/${pagePath}`, { waitUntil: 'domcontentloaded' });
await page.evaluate((u) => {
  const v = document.createElement('video');
  v.src = u;
  v.muted = true;
  v.autoplay = true;
  document.body.appendChild(v);
  window.__v = v;
}, url);
await new Promise((r) => setTimeout(r, secs * 1000));
const state = await page.evaluate(() => ({
  ready: window.__v.readyState,
  duration: Number.isFinite(window.__v.duration) ? window.__v.duration : null,
  t: Number(window.__v.currentTime.toFixed(2)),
  err: window.__v.error ? `${window.__v.error.code}:${window.__v.error.message}` : null,
}));

console.log(`object: ${url}  window=${windowBytes || 'unchanged'}  secs=${secs}`);
console.log('--- requests on the wire ---');
const t0 = Math.min(...[...reqs.values()].map((r) => r.t));
for (const [, r] of [...reqs].sort((a, b) => a[1].t - b[1].t)) {
  console.log(
    `  ${String(r.t - t0).padStart(6)} ms  Range: ${String(r.range).padEnd(16)} -> ${r.status} ` +
      `bytes=${String(r.bytes).padStart(10)} of ${r.contentLength}${r.transfer !== '-' ? ' te=' + r.transfer : ''} ` +
      `${r.contentRange} edge=${r.edge}`,
  );
  console.log(`          ${r.end ?? 'still open'}`);
}
console.log(`--- player state: ready=${state.ready} duration=${state.duration} t=${state.t} error=${state.err}`);
await ctx.close();
await browser.close();
