// The swarm: N viewer slots that keep replacing themselves for hours.
//
// Each session is the shape a real viewer has: open the player page, start
// playing, watch a few seconds, SEEK to somewhere else in the film, keep
// watching, then leave (the context dies and the slot is refilled). Slots are
// staggered at the start and jittered afterwards, so at any moment some viewers
// are cold-starting, some are mid-playback, some are seeking, some are leaving
// — which is the environment the origin has to survive.
//
// Every session is self-describing: the page's `window.__probe.snapshot()` is
// read back before the context is closed and appended as one JSON line, so the
// run can be accounted for even if the driver is killed afterwards.
//
//   node player-swarm.mjs --target https://cdn-oracle.isui.ren \
//     --page googledrive1/player-page.html --src miku-30min.m3u8 \
//     --slots 10 --hours 4.5 --play-secs 5 --watch-secs 600 --fanout 4 \
//     --window-secs 1800 --out /tmp/swarm-sessions.jsonl
//
// Nothing goes through a proxy (the browser is launched --no-proxy-server and
// node's fetch ignores HTTP_PROXY): every measurement is a direct one.

import { appendFileSync } from 'node:fs';
import { createRequire } from 'node:module';

const args = process.argv.slice(2);
const flag = (name, dflt = null) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? args[i + 1] : dflt;
};
const num = (name, dflt) => Number(flag(name, dflt));

const TARGET = (flag('target') || 'http://127.0.0.1:7779').replace(/\/$/, '');
// `--page` may be an absolute URL (a locally-served instrumented page); then
// TARGET is only used where the page itself points its media.
const PAGE = flag('page', 'media/player-page.html');
const SRC = flag('src', 'miku-30min.m3u8');
const SLOTS = num('slots', 10);
const HOURS = num('hours', 4.5);
const PLAY_SECS = num('play-secs', 5);
const WATCH_SECS = num('watch-secs', 600);
const FANOUT = num('fanout', 4);
const WINDOW_SECS = num('window-secs', 1800);
// The window's base: seek targets land inside [WINDOW_START, WINDOW_START +
// WINDOW_SECS], so every viewer shares ONE window of the content (the merge
// scenario). 0 = the content's beginning. With `--duration-secs` set, the
// targets and the opening positions are RANDOM over the whole duration instead
// (the truly-random shape), and WINDOW_* is ignored.
const WINDOW_START = num('window-start', 0);
const DURATION_SECS = num('duration-secs', 0);
// Asynchronous starts: each slot's FIRST session begins at t0 + uniform(0,
// ASYNC_START_SECS), independently drawn - the slots never line up, and the
// refills inherit the desynchronization (a session dies when ITS ten minutes
// are up, whenever that is).
const ASYNC_START_SECS = num('async-start-secs', 0);
const STAGGER_SECS = num('stagger-secs', 180);
const JITTER_SECS = num('jitter-secs', 60);
const STARTUP_TIMEOUT_SECS = num('startup-timeout-secs', 120);
const SESSION_BUDGET_SECS = num('session-budget-secs', PLAY_SECS + WATCH_SECS + 240);
const OUT = flag('out', '/tmp/swarm-sessions.jsonl');
const PROGRESS_SECS = num('progress-secs', 30);
const CHROME = process.env.CHROME || '/usr/bin/chromium';
const PW = flag('pw') || process.env.PW || process.env.PW_DIR;
const SEED = num('seed', Date.now() % 100000);

if (!PW) {
  console.error('need playwright-core: set --pw, PW or PW_DIR');
  process.exit(2);
}
const require = createRequire(import.meta.url);
const { chromium } = require(PW);

const t0 = Date.now();
const deadline = t0 + HOURS * 3600 * 1000;
const rel = (ms) => ((ms - t0) / 1000).toFixed(0) + 's';
const log = (...a) => console.log(`[${rel(Date.now())}]`, ...a);

// ---------------------------------------------------------------- randomness
let seed = SEED >>> 0;
const rnd = () => {
  seed = (seed * 1664525 + 1013904223) >>> 0;
  return seed / 2 ** 32;
};
const seekTarget = () => {
  // With a duration: random over the WHOLE video, leaving room for the watch
  // window after the seek. Without one: inside the shared window.
  if (DURATION_SECS > 0) {
    const hi = Math.max(1, DURATION_SECS - WATCH_SECS - 30);
    return Math.round(30 + rnd() * (hi - 30));
  }
  const lo = WINDOW_START + 30;
  const hi = Math.max(lo + 1, WINDOW_START + WINDOW_SECS - WATCH_SECS - 30);
  return Math.round(lo + rnd() * (hi - lo));
};
// The session's OPENING position (where the first 5 seconds play): random over
// the whole video when a duration is given, else the window's start.
const startOffset = () => {
  if (DURATION_SECS > 0) {
    const hi = Math.max(1, DURATION_SECS - 60);
    return Math.round(rnd() * hi);
  }
  return WINDOW_START;
};
const jitterMs = () => Math.round(rnd() * JITTER_SECS * 1000);

// ---------------------------------------------------------------- accounting
const stats = { started: 0, finished: 0, byOutcome: {}, bytes: 0, stalls: 0, seekFirst: [], seekResume: [], startup: [], fatal: 0 };
const note = (outcome) => (stats.byOutcome[outcome] = (stats.byOutcome[outcome] || 0) + 1);
const pct = (arr, p) => {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  return Math.round(s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))]);
};

function record(session) {
  appendFileSync(OUT, JSON.stringify(session) + '\n');
  stats.finished++;
  note(session.outcome);
  if (session.bytes) stats.bytes += session.bytes;
  if (session.stalls) stats.stalls += session.stalls;
  if (session.fatalErrors) stats.fatal += session.fatalErrors;
  if (session.startupMs != null) stats.startup.push(session.startupMs);
  if (session.seekFirstByteMs != null) stats.seekFirst.push(session.seekFirstByteMs);
  if (session.seekResumeMs != null) stats.seekResume.push(session.seekResumeMs);
}

function progress(running) {
  log(
    `slots=${running}/${SLOTS} sessions=${stats.finished} (${Object.entries(stats.byOutcome)
      .map(([k, v]) => `${k}:${v}`)
      .join(' ') || 'none'}) ` +
      `bytes=${(stats.bytes / 2 ** 30).toFixed(2)} GiB stalls=${stats.stalls} fatal=${stats.fatal} ` +
      `startup p50=${pct(stats.startup, 50)}ms p90=${pct(stats.startup, 90)}ms ` +
      `seekTTFB p50=${pct(stats.seekFirst, 50)}ms p90=${pct(stats.seekFirst, 90)}ms ` +
      `seekResume p50=${pct(stats.seekResume, 50)}ms`,
  );
}

// ---------------------------------------------------------------- one session
async function session(browser, slot, id) {
  const target = seekTarget();
  const start = startOffset();
  const url =
    `${PAGE.startsWith('http') ? PAGE : `${TARGET}/${PAGE}`}` +
    `?src=${encodeURIComponent(SRC)}&start=${start}&seek-after=${PLAY_SECS}&seek=${target}&watch=${WATCH_SECS}` +
    (FANOUT > 0 ? `&fanout=${FANOUT}` : '');
  const started = Date.now();
  const rec = {
    slot,
    session: id,
    startedAt: new Date(started).toISOString(),
    start,
    target,
    url,
    outcome: 'unknown',
    startupMs: null,
    seekFirstByteMs: null,
    seekResumeMs: null,
    seekFragStart: null,
    stalls: 0,
    stalledMs: 0,
    playedMs: 0,
    bytes: 0,
    frags: 0,
    fatalErrors: 0,
    errors: [],
    stopReason: null,
    currentTime: null,
    buffered: 0,
    wallMs: 0,
  };
  let ctx = null;
  try {
    ctx = await browser.newContext({ viewport: { width: 640, height: 400 } });
    const page = await ctx.newPage();
    page.on('pageerror', (e) => rec.errors.push(`pageerror: ${String(e).slice(0, 160)}`));
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });

    let snap = null;
    while (Date.now() - started < SESSION_BUDGET_SECS * 1000) {
      await new Promise((r) => setTimeout(r, 5000));
      snap = await page.evaluate(() => (window.__probe && window.__probe.snapshot ? window.__probe.snapshot() : null)).catch(() => null);
      if (!snap) continue;
      if (snap.startupMs === null && (Date.now() - started) / 1000 > STARTUP_TIMEOUT_SECS) {
        rec.outcome = 'startup_failed';
        break;
      }
      if (snap.done) {
        rec.outcome = snap.stopReason === 'watched' ? 'watched' : snap.stopReason || 'done';
        break;
      }
    }
    if (rec.outcome === 'unknown') rec.outcome = 'budget_timeout';
    if (snap) {
      rec.startupMs = snap.startupMs;
      rec.seekFirstByteMs = snap.seek ? snap.seek.firstByteMs : null;
      rec.seekResumeMs = snap.seek ? snap.seek.resumeMs : null;
      rec.seekFragStart = snap.seek ? snap.seek.fragStart : null;
      rec.stalls = snap.stalls;
      rec.stalledMs = snap.stalledMs;
      rec.playedMs = snap.playedMs;
      rec.playedContentSecs = snap.currentTime;
      rec.bytes = snap.bytes;
      rec.frags = snap.frags;
      rec.fatalErrors = snap.fatalErrors;
      rec.errors = (snap.errors || []).slice(0, 3).map((e) => `${e.type}/${e.details}${e.fatal ? ' (fatal)' : ''}`);
      rec.stopReason = snap.stopReason;
      rec.currentTime = snap.currentTime;
      rec.buffered = (snap.buffered || []).length;
    }
  } catch (err) {
    rec.outcome = 'driver_error';
    rec.errors.push(String(err).slice(0, 200));
  } finally {
    if (ctx) await ctx.close().catch(() => {});
    rec.wallMs = Date.now() - started;
    rec.endedAt = new Date().toISOString();
    record(rec);
  }
}

// ---------------------------------------------------------------- driver
let browser = await chromium.launch({
  executablePath: CHROME,
  args: [
    '--no-proxy-server',
    '--no-sandbox',
    '--disable-dev-shm-usage',
    '--autoplay-policy=no-user-gesture-required',
    '--mute-audio',
    `--window-size=640,400`,
  ],
  headless: true,
});
log(`target ${TARGET}/${PAGE}  src=${SRC}  slots=${SLOTS} hours=${HOURS} fanout=${FANOUT} seed=${SEED}`);
log(`sessions -> ${OUT}`);

let stopping = false;
const stop = async (why) => {
  if (stopping) return;
  stopping = true;
  log(`stopping (${why})`);
  await browser.close().catch(() => {});
  log('final:');
  progress(0);
  process.exit(0);
};
process.on('SIGINT', () => stop('SIGINT'));
process.on('SIGTERM', () => stop('SIGTERM'));

let counter = 0;
const slotLoop = async (slot) => {
  // Asynchronous by construction: with ASYNC_START_SECS each slot draws its
  // own uniform delay, so ten slots never line up; with STAGGER_SECS the slots
  // still spread (i * stagger). Refills happen per slot whenever the previous
  // session dies, which keeps them staggered for the whole run.
  const startAt =
    ASYNC_START_SECS > 0
      ? t0 + rnd() * ASYNC_START_SECS * 1000
      : t0 + slot * STAGGER_SECS * 1000;
  if (startAt > Date.now()) await new Promise((r) => setTimeout(r, startAt - Date.now()));
  while (!stopping && Date.now() < deadline) {
    if (!browser.isConnected()) {
      log('browser died; relaunching');
      browser = await chromium.launch({
        executablePath: CHROME,
        args: ['--no-proxy-server', '--no-sandbox', '--disable-dev-shm-usage', '--autoplay-policy=no-user-gesture-required', '--mute-audio'],
        headless: true,
      });
    }
    stats.started++;
    await session(browser, slot, ++counter);
    const wait = jitterMs();
    if (Date.now() + wait >= deadline) break;
    await new Promise((r) => setTimeout(r, wait));
  }
};

const ticker = setInterval(() => progress(SLOTS), PROGRESS_SECS * 1000);
await Promise.all(Array.from({ length: SLOTS }, (_, i) => slotLoop(i)));
clearInterval(ticker);
await stop('deadline');
