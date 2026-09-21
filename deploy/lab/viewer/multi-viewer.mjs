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
// `--objects a,b,c` gives each viewer its OWN object (the permit-queue case).
// `--unique-seeds` gives each viewer its own jump positions (the one-pin-per-key
// case: several viewers on ONE key at DIFFERENT places).
const objects = (arg('objects', '') || '').split(',').filter(Boolean);
const uniqueSeeds = args.includes('--unique-seeds');
const chrome = process.env.CHROME || '/usr/bin/chromium';
const pwDir = process.env.PW || '/home/archivalera/.npm/_npx/9833c18b2d85bc59/node_modules/playwright-core';

const { chromium } = await import(join(pwDir, 'index.mjs')).catch(async () => await import(pwDir));
const readerSrc = readFileSync(join(here, 'reader.js'), 'utf8');

const base = target.replace(/\/$/, '');
const pathFor = (i) => (objects.length ? objects[i % objects.length] : objectPath);
const url = `${base}/${objectPath}?size=${size}`;
const pageUrl = `${base}/${pagePath}`;

const browser = await chromium.launch({ executablePath: chrome, args: ['--no-proxy-server'] });
// Pages first, ONE AT A TIME. Three concurrent GETs of the SAME small object is
// the one shape the edge in front of this origin stalls — measured against the
// real CDN: of three concurrent page loads, two were served in 0.1-0.7 s and one
// waited 8.7 s, 11.9 s and once over 300 s, with the origin's own counters flat
// the whole time (no open, no stat, no session). A reader is not a page load:
// the concurrency this harness exists to measure starts after the warm-up, and
// its wall clock starts there too.
const pages = [];
for (let i = 0; i < viewers; i++) {
  const ctx = await browser.newContext(); // isolated: its own cache and pool
  const page = await ctx.newPage();
  // The page must be same-origin with the object, so the reader's fetch needs
  // no CORS header. It is an ordinary small object the target serves.
  await page.goto(pageUrl, { waitUntil: 'domcontentloaded' });
  pages.push({ ctx, page });
}
const started = Date.now();
const results = await Promise.all(
  Array.from({ length: viewers }, async (_, i) => {
    const { ctx, page } = pages[i];
    // The reader is EVALUATED, not injected as a <script> tag: an origin (or a
    // CDN in front of it) may send a Content-Security-Policy that blocks inline
    // scripts, and devtools-style evaluation is not subject to CSP. Same code
    // path for every target.
    const opts = {
      chunkBytes,
      chunks,
      seeks,
      gapMs,
      url: objects.length ? `${base}/${pathFor(i)}?size=${size}` : url,
      seed: uniqueSeeds ? 12345 + i * 7919 : 12345,
    };
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
