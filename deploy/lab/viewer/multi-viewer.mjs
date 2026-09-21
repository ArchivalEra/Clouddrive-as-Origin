// N independent viewers of one large object, driven in real Chromium.
//
// Why browsers and not curl: a viewer is a browser — several connections, its
// own cache, its own scheduling — and what this measures is what a viewer feels
// (the gap between bytes) alongside what the origin pays (its upstream opens).
// Each viewer gets its OWN browser context, so no two share a cache, a
// connection pool or a media buffer.
//
// The page each viewer runs in is an object served by the TARGET origin, so the
// harness needs no CORS headers and nothing is uploaded: the reader script is
// injected into that page's context.
//
// Usage:
//   node multi-viewer.mjs --target <base-url> --object <path> --size <bytes>
//                         [--page <path-of-an-existing-small-object>]
//   `--object` and `--page` are paths under the target (the LAB serves
//   `media/…`, the CDN serves `googledrive1/…`), not bare keys.
//                         [--viewers 4] [--chunks 64] [--chunk-bytes 262144]
//                         [--seeks 4] [--gap-ms 1500]
//   Env: CHROME=/usr/bin/chromium  PW=<playwright-core dir>
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const arg = (name, def) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 && args[i + 1] ? args[i + 1] : def;
};
const target = arg('target', 'http://127.0.0.1:7779');
const objectPath = arg('object', 'media/viewer-object.bin');
const pagePath = arg('page', 'media/hello.txt');
const viewers = Number(arg('viewers', '4'));
const chunks = Number(arg('chunks', '64'));
const chunkBytes = Number(arg('chunk-bytes', String(256 * 1024)));
const seeks = Number(arg('seeks', '4'));
const gapMs = Number(arg('gap-ms', '1500'));
const size = Number(arg('size', '0'));
const chrome = process.env.CHROME || '/usr/bin/chromium';
const pwDir = process.env.PW || '/home/archivalera/.npm/_npx/9833c18b2d85bc59/node_modules/playwright-core';

const { chromium } = await import(join(pwDir, 'index.mjs')).catch(async () => await import(pwDir));
const readerSrc = readFileSync(join(here, 'reader.js'), 'utf8');

const base = target.replace(/\/$/, '');
const url = `${base}/${objectPath}?size=${size}`;
const pageUrl = `${base}/${pagePath}`;

const browser = await chromium.launch({ executablePath: chrome, args: ['--no-proxy-server'] });
const started = Date.now();
const results = await Promise.all(
  Array.from({ length: viewers }, async (_, i) => {
    const ctx = await browser.newContext(); // isolated: its own cache and pool
    const page = await ctx.newPage();
    // The page must be same-origin with the object, so the reader's fetch needs
    // no CORS header. It is an ordinary small object the target serves.
    await page.goto(pageUrl, { waitUntil: 'domcontentloaded' });
    // The reader is EVALUATED, not injected as a <script> tag: an origin (or a
    // CDN in front of it) may send a Content-Security-Policy that blocks inline
    // scripts, and devtools-style evaluation is not subject to CSP. Same code
    // path for every target.
    const opts = { chunkBytes, chunks, seeks, gapMs, url };
    const stats = await page.evaluate(
      `(async () => { ${readerSrc}\n return await window.__readRange(${JSON.stringify(opts)}); })()`,
    );
    await ctx.close();
    return { viewer: i, ...stats };
  }),
);
await browser.close();

const wall = Date.now() - started;
const sum = (f) => results.reduce((a, r) => a + f(r), 0);
const ttfb = results.flatMap((r) => r.seekTtfbMs || []);
ttfb.sort((a, b) => a - b);
const p = (q) => (ttfb.length ? ttfb[Math.min(ttfb.length - 1, Math.floor(ttfb.length * q))] : 0);

console.log(`viewers=${viewers} target=${base} object=${objectPath} size=${size}`);
for (const r of results) {
  console.log(
    `  viewer ${r.viewer}: bytes=${r.bytes} requests=${r.requests} gaps>${gapMs}ms=${r.gaps} ` +
      `worst=${r.worstGapMs}ms sum=${r.gapTotalMs}ms seekTTFB p50=${r.seekTtfbMs?.length ? r.seekTtfbMs.slice().sort((a, b) => a - b)[Math.floor(r.seekTtfbMs.length / 2)] : '-'}ms ` +
      `ck=${r.checksum}${r.error ? ' ERROR=' + r.error : ''}${r.aborted ? ' (capped)' : ''}`,
  );
}
console.log(
  `total: bytes=${sum((r) => r.bytes)} requests=${sum((r) => r.requests)} gaps=${sum((r) => r.gaps)} ` +
    `gapTotalMs=${sum((r) => r.gapTotalMs)} worstGapMs=${Math.max(...results.map((r) => r.worstGapMs))} ` +
    `seekTTFB p50=${p(0.5)}ms p90=${p(0.9)}ms wall=${wall}ms errors=${results.filter((r) => r.error).length} ` +
    `checksums=${new Set(results.map((r) => r.checksum)).size}`,
);
