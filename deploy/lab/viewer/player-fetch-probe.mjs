// Drive the ready-made player page in real Chromium and report what it did:
// how far it played, which fragments it loaded, how many loaded in parallel
// (overlapping load windows), and any error it hit.
//
//   node player-fetch-probe.mjs <page-url-with-?src=> [seconds-to-watch] [progress-secs]
//
// A session measured in hours has to be observable while it runs: with
// progress-secs > 0 this prints the video element's own state every interval, so
// a run that stops advancing is visible instead of only visible at the end.
import { join } from 'node:path';

const pageUrl = process.argv[2];
const watchSecs = Number(process.argv[3] || 20);
const progressSecs = Number(process.argv[4] || 0);
const pwDir = process.env.PW || '/home/archivalera/.npm/_npx/9833c18b2d85bc59/node_modules/playwright-core';
const chrome = process.env.CHROME || '/usr/bin/chromium';
const { chromium } = await import(join(pwDir, 'index.mjs')).catch(async () => await import(pwDir));

const browser = await chromium.launch({ executablePath: chrome, args: ['--no-proxy-server'] });
const ctx = await browser.newContext();
const page = await ctx.newPage();
const requests = [];
page.on('request', (r) => requests.push({ url: r.url(), range: r.headers()['range'] || '', at: Date.now() }));

const started = Date.now();
const timer = progressSecs > 0
  ? setInterval(async () => {
      const since = Math.round((Date.now() - started) / 1000);
      try {
        const s = await page.evaluate(() => {
          const v = document.querySelector('video');
          const p = window.__probe || {};
          return {
            readyState: v.readyState,
            currentTime: Number(v.currentTime.toFixed(1)),
            bufferedEnd: v.buffered.length ? Number(v.buffered.end(v.buffered.length - 1).toFixed(1)) : 0,
            stalls: (p.events || []).filter((e) => e.ev === 'waiting' || e.ev === 'stalled').length,
            errors: (p.errors || []).length,
          };
        });
        console.log(`  [progress +${since}s] currentTime=${s.currentTime}s bufferedEnd=${s.bufferedEnd}s readyState=${s.readyState} stalls=${s.stalls} errors=${s.errors}`);
      } catch (e) {
        console.log(`  [progress +${since}s] unreadable: ${e.message}`);
      }
    }, progressSecs * 1000)
  : null;

await page.goto(pageUrl, { waitUntil: 'domcontentloaded' });
await page.waitForTimeout(watchSecs * 1000);
if (timer) clearInterval(timer);

const state = await page.evaluate(() => {
  const v = document.querySelector('video');
  const p = window.__probe || { events: [], frags: [], errors: [] };
  const overlaps = (a, b) => a.start < b.end && b.start < a.end;
  let parallelPairs = 0;
  for (let i = 0; i < p.frags.length; i++)
    for (let j = i + 1; j < p.frags.length; j++) if (overlaps(p.frags[i], p.frags[j])) parallelPairs++;
  const media = performance
    .getEntriesByType('resource')
    .filter((r) => r.name.includes('.m4s') || r.name.includes('hls'))
    .map((r) => ({
      bytes: r.transferSize || r.encodedBodySize || 0,
      startMs: Math.round(r.startTime),
      durMs: Math.round(r.duration),
    }));
  return {
    readyState: v.readyState,
    currentTime: Number(v.currentTime.toFixed(2)),
    duration: Number.isFinite(v.duration) ? Number(v.duration.toFixed(1)) : null,
    paused: v.paused,
    buffered: v.buffered.length ? [Number(v.buffered.start(0).toFixed(1)), Number(v.buffered.end(v.buffered.length - 1).toFixed(1))] : null,
    events: p.events,
    frags: p.frags.map((f) => ({ sn: f.sn, bytes: f.bytes, start: f.start, first: f.first, end: f.end })),
    errors: p.errors,
    parallelPairs,
    media,
  };
});
await browser.close();

const bytes = state.frags.reduce((a, f) => a + (f.bytes || 0), 0);
console.log(`readyState=${state.readyState} currentTime=${state.currentTime}s duration=${state.duration}s paused=${state.paused}`);
console.log(`buffered=${JSON.stringify(state.buffered)} frags=${state.frags.length} bytes=${bytes} parallelOverlappingLoads=${state.parallelPairs}`);
console.log(`frags: ${JSON.stringify(state.frags)}`);
console.log(`media resource entries (${state.media.length}): ${JSON.stringify(state.media)}`);
console.log(`events=${JSON.stringify(state.events)}`);
console.log(`errors=${JSON.stringify(state.errors)}`);
const starts = requests.filter((r) => r.range);
console.log(`ranged requests=${starts.length}: ${starts.slice(0, 8).map((r) => r.range).join(' | ')}${starts.length > 8 ? ' ...' : ''}`);
