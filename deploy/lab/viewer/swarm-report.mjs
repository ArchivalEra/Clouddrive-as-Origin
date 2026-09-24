// Turn a swarm run's JSONL into the account: outcomes, latencies, stalls, and
// the interleaving (how many viewers were in the film at once, and when).
//
//   node swarm-report.mjs /tmp/swarm-sessions.jsonl [--bucket-minutes 10]
//
// Every number is read from the sessions themselves; nothing is re-measured.

import { readFileSync } from 'node:fs';

const file = process.argv[2];
if (!file) {
  console.error('usage: swarm-report.mjs <sessions.jsonl> [--bucket-minutes N]');
  process.exit(2);
}
const bi = process.argv.indexOf('--bucket-minutes');
const BUCKET_MIN = bi >= 0 ? Number(process.argv[bi + 1]) : 10;

const rows = readFileSync(file, 'utf8')
  .split('\n')
  .filter((l) => l.trim())
  .map((l) => JSON.parse(l));

if (!rows.length) {
  console.log('no sessions');
  process.exit(0);
}
rows.sort((a, b) => a.startedAt.localeCompare(b.startedAt));

const pct = (arr, p) => {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  return Math.round(s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))]);
};
const num = (v) => (v == null ? null : v);
const sum = (arr) => arr.reduce((a, b) => a + b, 0);
const fmt = (v) => (v == null ? '-' : String(v));

const outcomes = {};
for (const r of rows) outcomes[r.outcome] = (outcomes[r.outcome] || 0) + 1;
const startup = rows.map((r) => num(r.startupMs)).filter((v) => v != null);
const seekFirst = rows.map((r) => num(r.seekFirstByteMs)).filter((v) => v != null);
const seekResume = rows.map((r) => num(r.seekResumeMs)).filter((v) => v != null);
const played = rows.map((r) => num(r.playedMs) || 0);
const stalls = rows.map((r) => r.stalls || 0);
const bytes = rows.map((r) => r.bytes || 0);
const wall = (Date.parse(rows.at(-1).endedAt) - Date.parse(rows[0].startedAt)) / 1000;

console.log(`sessions   : ${rows.length} over ${(wall / 3600).toFixed(2)} h`);
console.log(
  `outcomes   : ${Object.entries(outcomes)
    .sort((a, b) => b[1] - a[1])
    .map(([k, v]) => `${k}=${v} (${((100 * v) / rows.length).toFixed(0)}%)`)
    .join('  ')}`,
);
console.log(`startup    : p50=${fmt(pct(startup, 50))}ms p90=${fmt(pct(startup, 90))}ms max=${startup.length ? Math.max(...startup) : '-'}ms  (n=${startup.length})`);
console.log(`seek TTFB  : p50=${fmt(pct(seekFirst, 50))}ms p90=${fmt(pct(seekFirst, 90))}ms max=${seekFirst.length ? Math.max(...seekFirst) : '-'}ms  (n=${seekFirst.length})`);
console.log(`seek resume: p50=${fmt(pct(seekResume, 50))}ms p90=${fmt(pct(seekResume, 90))}ms  (n=${seekResume.length})`);
console.log(`played     : total ${(sum(played) / 3600000).toFixed(2)} h, median ${(pct(played, 50) / 1000).toFixed(0)}s per session`);
console.log(`bytes      : ${(sum(bytes) / 2 ** 30).toFixed(2)} GiB, median ${(pct(bytes, 50) / 2 ** 20).toFixed(1)} MiB per session`);
console.log(
  `stalls     : ${sum(stalls)} across sessions; ${stalls.filter((s) => s > 0).length}/${rows.length} sessions stalled; ` +
    `p50=${pct(stalls, 50)} p90=${pct(stalls, 90)} max=${Math.max(...stalls)}`,
);
const withFatal = rows.filter((r) => r.fatalErrors > 0).length;
console.log(`fatal hls  : ${withFatal} sessions with a fatal error`);
const errKinds = {};
for (const r of rows) {
  for (const e of r.errors || []) {
    const k = String(e).split('\n')[0].slice(0, 120);
    errKinds[k] = (errKinds[k] || 0) + 1;
  }
}
if (Object.keys(errKinds).length) {
  console.log('top errors :');
  for (const [k, v] of Object.entries(errKinds).sort((a, b) => b[1] - a[1]).slice(0, 6)) {
    console.log(`  ${String(v).padStart(4)}  ${k}`);
  }
}

// ---- interleaving: how many viewers were inside the film at once
const events = [];
for (const r of rows) {
  events.push({ t: Date.parse(r.startedAt), d: +1 });
  events.push({ t: Date.parse(r.endedAt), d: -1 });
}
events.sort((a, b) => a.t - b.t || a.d - b.d);
let cur = 0;
let maxConc = 0;
const concSamples = [];
for (const e of events) {
  cur += e.d;
  maxConc = Math.max(maxConc, cur);
  concSamples.push({ t: e.t, c: cur });
}
const avgConc = concSamples.length ? sum(concSamples.map((s) => s.c)) / concSamples.length : 0;
console.log(`concurrency: max=${maxConc} average=${avgConc.toFixed(2)} (over ${concSamples.length} state changes)`);

// ---- timeline buckets: sessions started / bytes / stalls / seeks inside each
const start = Date.parse(rows[0].startedAt);
const buckets = new Map();
const bucketOf = (ms) => Math.floor((ms - start) / (BUCKET_MIN * 60000));
for (const r of rows) {
  const k = bucketOf(Date.parse(r.startedAt));
  const b = buckets.get(k) || { n: 0, bytes: 0, stalls: 0, starts: 0, fails: 0 };
  b.n++;
  b.bytes += r.bytes || 0;
  b.stalls += r.stalls || 0;
  if ((r.startupMs ?? 0) > 30000) b.fails++;
  buckets.set(k, b);
}
console.log(`\ntimeline (${BUCKET_MIN}-minute buckets from the first session):`);
console.log('  bucket   sessions   GiB    stalls  slow-starts');
for (const k of [...buckets.keys()].sort((a, b) => a - b)) {
  const b = buckets.get(k);
  console.log(
    `  ${String(k * BUCKET_MIN).padStart(4)}min  ${String(b.n).padStart(6)}  ${(b.bytes / 2 ** 30).toFixed(2).padStart(6)}  ${String(b.stalls).padStart(6)}  ${String(b.fails).padStart(6)}`,
  );
}

// ---- per-slot
console.log('\nper slot: sessions / stalled / bytes GiB');
const slots = new Map();
for (const r of rows) {
  const s = slots.get(r.slot) || { n: 0, stalled: 0, bytes: 0 };
  s.n++;
  if ((r.stalls || 0) > 0) s.stalled++;
  s.bytes += r.bytes || 0;
  slots.set(r.slot, s);
}
for (const k of [...slots.keys()].sort((a, b) => a - b)) {
  const s = slots.get(k);
  console.log(`  slot ${String(k).padStart(2)}: ${String(s.n).padStart(3)} / ${String(s.stalled).padStart(3)} / ${(s.bytes / 2 ** 30).toFixed(2)}`);
}
