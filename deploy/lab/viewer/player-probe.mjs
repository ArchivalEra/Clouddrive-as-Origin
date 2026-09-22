// A real PLAYER's request shape, measured in real Chromium.
//
// Why this exists next to `multi-viewer.mjs`: that harness reads byte ranges the
// way a generic client does — the shape the origin was designed around, but not
// the shape a browser actually produces when it decides for itself. A `<video>`
// element chooses its own ranges, in its own order, with its own retries, and
// the origin's whole design (windows, runs, the chain, the escapes) is a bet
// about what those requests look like. This probe records the bet's other side:
// the requests the player made, when, and how big — plus the gaps the viewer
// felt (`waiting`/`stalled`/`error` events).
//
// The browser's own range requests are invisible to page JavaScript, so they are
// read from the resource timing buffer instead: every entry whose initiator is
// the video carries its start time, duration and transferred size. That is the
// player's request shape, straight from the browser.
//
// Usage:
//   node player-probe.mjs --target <base-url> --object <path> [--page <path>]
//                         [--play-secs <n>] [--timeout-secs <n>]
//                         [--progress-secs <n>]
//   `--object`/`--page` are paths under the target (the LAB serves `media/…`),
//   not bare keys. `--play-secs` bounds the listening window (default 30, the
//   length of the LAB's own clip); the probe stops early on `ended` or `error`.
//   Env: CHROME=/usr/bin/chromium  PW=<playwright-core dir>
//
// Output: one line per player event, then a table of the requests the player
// made, then the summary (bytes, requests, the largest gap between two request
// starts, and whether playback stalled).

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
const objectPath = arg('object', 'media/viewer-object.mp4');
const pagePath = arg('page', 'media/hello.txt');
const playSecs = Number(arg('play-secs', '30'));
const timeoutSecs = Number(arg('timeout-secs', String(playSecs + 20)));
const progressSecs = Number(arg('progress-secs', '0'));
const chrome = process.env.CHROME || '/usr/bin/chromium';
const pwDir = process.env.PW || '/home/archivalera/.npm/_npx/9833c18b2d85bc59/node_modules/playwright-core';

const { chromium } = await import(join(pwDir, 'index.mjs')).catch(async () => await import(pwDir));

const base = target.replace(/\/$/, '');
const objectUrl = `${base}/${objectPath}`;

const browser = await chromium.launch({ executablePath: chrome, args: ['--no-proxy-server'] });
const ctx = await browser.newContext();
const page = await ctx.newPage();
// The page must be same-origin with the object, so the video's own requests need
// no CORS header. It is an ordinary small object the target serves.
await page.goto(`${base}/${pagePath}`, { waitUntil: 'domcontentloaded' });

// The browser's media requests do not go through the page's own fetch, so page
// JavaScript cannot see them and resource timing reports sizes without the
// REQUEST (no Range header, no offsets). CDP can: it sees every request the
// browser makes, media included, with its headers.
const cdp = await ctx.newCDPSession(page);
const wire = [];
let wireAt = (u) => u.includes(new URL(objectUrl).pathname);
await cdp.send('Network.enable');
cdp.on('Network.requestWillBeSent', (e) => {
  if (wireAt(e.request.url)) wire.push({ id: e.requestId, at: Date.now(), range: e.request.headers['Range'] ?? null });
});
cdp.on('Network.responseReceived', (e) => {
  const r = wire.find((w) => w.id === e.requestId);
  if (!r) return;
  r.status = e.response.status;
  r.contentRange = e.response.headers['content-range'] ?? null;
  r.contentLength = Number(e.response.headers['content-length'] ?? 0);
  r.edge = e.response.headers['eo-cache-status'] ?? null;
});
cdp.on('Network.loadingFinished', (e) => {
  const r = wire.find((w) => w.id === e.requestId);
  if (r) r.ms = Date.now() - r.at;
});

// A long session needs to be observable WHILE it runs: one line per interval
// with the player's own state and what the wire has carried so far. Read from
// Node (not from inside the page) so the reporting cannot perturb the run.
const progress = progressSecs > 0
  ? setInterval(async () => {
      try {
        const state = await page.evaluate(() => {
          const v = document.querySelector('video');
          if (!v) return null;
          return { ready: v.readyState, t: v.currentTime, dur: Number.isFinite(v.duration) ? v.duration : null };
        });
        const answered = wire.filter((w) => w.status).length;
        const declared = wire.reduce((a, w) => a + (w.contentLength || 0), 0);
        const hits = wire.filter((w) => (w.edge || '').toUpperCase().includes('HIT')).length;
        const misses = wire.filter((w) => (w.edge || '').toUpperCase().includes('MISS')).length;
        const started = state && state.ready >= 2;
        console.log(
          `  [progress +${Math.round(process.uptime())}s] ` +
            `t=${state ? state.t.toFixed(0) : '?'}s ready=${state ? state.ready : '?'} ` +
            `reqs=${wire.length} answered=${answered} declared_bytes=${declared} ` +
            `edgeHIT=${hits} edgeMISS=${misses}` +
            (started ? '' : '  <- NOT PLAYING'),
        );
      } catch (e) {
        console.log(`  [progress] unreadable: ${e.message}`);
      }
    }, progressSecs * 1000)
  : null;

const report = await page.evaluate(
  async ({ url, playSecs, timeoutSecs }) => {
    const events = [];
    const t0 = performance.now();
    const stamp = () => Math.round(performance.now() - t0);
    // The browser's own requests for this video, from the resource timing buffer.
    const seen = new Set();
    const requests = [];
    const collect = () => {
      for (const e of performance.getEntriesByType('resource')) {
        if (!e.name.includes(new URL(url).pathname)) continue;
        const key = `${e.name}|${e.startTime}`;
        if (seen.has(key)) continue;
        seen.add(key);
        requests.push({
          at: Math.round(e.startTime - t0),
          ms: Math.round(e.duration),
          bytes: e.transferSize || e.encodedBodySize || 0,
        });
      }
    };
    new PerformanceObserver(collect).observe({ entryTypes: ['resource'] });

    const v = document.createElement('video');
    v.src = url;
    v.muted = true;
    v.autoplay = true;
    v.playsInline = true;
    document.body.appendChild(v);
    let firstPlaying = null;
    for (const name of ['loadedmetadata', 'canplay', 'playing', 'waiting', 'stalled', 'error', 'ended', 'suspend']) {
      v.addEventListener(name, () => {
        const at = stamp();
        if (name === 'playing' && firstPlaying === null) firstPlaying = at;
        events.push({ at, name, ready: v.readyState, t: Number(v.currentTime.toFixed(2)) });
      });
    }
    const done = new Promise((resolve) => {
      const stop = () => resolve();
      v.addEventListener('ended', stop);
      v.addEventListener('error', stop);
      setTimeout(stop, timeoutSecs * 1000);
    });
    await Promise.race([
      v.play().catch((e) => events.push({ at: stamp(), name: `play-rejected:${e.name}`, ready: v.readyState, t: 0 })),
      new Promise((r) => setTimeout(r, 500)),
    ]);
    await done;
    collect();
    const gaps = requests.slice(1).map((r, i) => r.at - requests[i].at);
    return {
      events,
      requests,
      played: Number(v.currentTime.toFixed(2)),
      duration: Number.isFinite(v.duration) ? Number(v.duration.toFixed(2)) : null,
      ready: v.readyState,
      maxGapMs: gaps.length ? Math.max(...gaps) : 0,
      // A `waiting` BEFORE the first `playing` is startup buffering, not a stall:
      // every player does it, and counting it would fail every run. The stall
      // this measures is the one a viewer feels — playback that stops.
      stalled: events.filter(
        (e) => (e.name === 'waiting' || e.name === 'stalled') && firstPlaying !== null && e.at > firstPlaying,
      ).length,
      startupWaits: events.filter((e) => e.name === 'waiting' && (firstPlaying === null || e.at <= firstPlaying))
        .length,
      errored: events.filter((e) => e.name.startsWith('error') || e.name.startsWith('play-rejected')).length,
    };
  },
  { url: objectUrl, playSecs, timeoutSecs },
);

// The session is over: the reporter is the only thing still pending.
if (progress) clearInterval(progress);
await ctx.close();
await browser.close();

console.log(`object: ${objectUrl}  play_secs=${playSecs}`);
console.log('--- events ---');
for (const e of report.events) {
  console.log(`  ${String(e.at).padStart(6)} ms  ${e.name.padEnd(20)} ready=${e.ready} t=${e.t}s`);
}
console.log('--- the requests the PLAYER made (CDP: the real shape) ---');
const wireBase = wire.length ? wire[0].at : 0;
for (const w of wire) {
  console.log(
    `  ${String(w.at - wireBase).padStart(6)} ms  ${w.range ?? '(no Range)'}  -> ${w.status} ${
      w.contentRange ?? ''
    } edge=${w.edge ?? '-'} ${w.ms ?? '?'} ms`,
  );
}
console.log(`  ${wire.length} request(s), ${wire.filter((w) => w.range).length} with a Range header`);
console.log('--- sizes from resource timing (cross-check) ---');
for (const r of report.requests) {
  console.log(`  ${String(r.at).padStart(6)} ms  ${String(r.ms).padStart(6)} ms  ${String(r.bytes).padStart(9)} bytes`);
}
const bytes = report.requests.reduce((a, r) => a + r.bytes, 0);
console.log(
  `summary: requests=${report.requests.length} bytes=${bytes} played=${report.played}s/${
    report.duration ?? '?'
  }s ready=${report.ready} stalls=${report.stalled} startupWaits=${report.startupWaits} errors=${
    report.errored
  } maxGapBetweenRequests=${report.maxGapMs}ms`,
);
// The verdict is about the PLAYER'S EXPERIENCE, not about how far the bounded
// window let it get: an error or a stall is a failure, and playing the window
// out without either is a pass. `--play-secs` shorter than the clip is the normal
// case (the point is the request shape, not watching a 30 s clip).
if (report.ready === 0) {
  // `readyState` 0 is HAVE_NOTHING: no metadata, so playback never began. That
  // is a failure however calm the event log looks — and it is the first thing a
  // viewer would report.
  console.log('VERDICT: the player never loaded metadata (playback never began)');
} else if (report.errored > 0) {
  console.log('VERDICT: the player hit an error');
} else if (report.stalled > 0) {
  console.log(`VERDICT: the player stalled ${report.stalled} time(s)`);
} else if (report.duration !== null && report.played >= report.duration - 0.5) {
  console.log('VERDICT: the player played it through');
} else {
  console.log(`VERDICT: no stall and no error over ${report.played}s of playback`);
}
