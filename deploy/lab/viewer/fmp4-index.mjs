// Build an HLS byte-range playlist for a REMOTE fragmented-MP4 object.
//
// Why this exists: hls.js plays segments, not files, and the product's object
// is one 200 GiB fMP4 with no `sidx` and no index at the tail (measured: the
// last 4 MiB carry `moof`/`mdat` only). What IS in the file is enough to build
// the playlist by hand: every `moof` states its own size and carries its
// samples' durations, and the `mdat` that follows states its size too. So a
// prefix of the film becomes `#EXT-X-BYTERANGE` entries without re-encoding
// anything and WITHOUT downloading the object: the walk reads one small window
// around each `moof` (~1 KiB) and skips every `mdat`'s payload by its header.
//
// The playlist's URIs point at the ORIGINAL object (same directory), so the
// player's ranges are served by the origin exactly like any other ranged read.
//
//   node fmp4-index.mjs --url <film-url> --probe [--tail-bytes N]
//   node fmp4-index.mjs --url <film-url> --minutes 30 --seg-secs 2 --out <dir>
//   node fmp4-index.mjs --in <local-prefix-file> --film-name <uri> --out <dir>
//
// Fetching uses node's `fetch`, which does NOT honour HTTP_PROXY: this is a
// direct measurement, the same rule the rest of the lab follows.
//
// Self-assertions (a playlist that fails one is not written):
//   * every segment lies inside the object, is non-empty, and does not overlap
//     its neighbour; the segments are strictly increasing
//   * baseMediaDecodeTime never goes backwards
//   * sum(EXTINF) equals the walked duration and covers >= the asked window
//   * the init segment really is the `ftyp`+`moov` byte range at the head

import { writeFile, mkdir } from 'node:fs/promises';
import { readFileSync, createWriteStream, openSync, writeSync, closeSync } from 'node:fs';
import path from 'node:path';

const args = process.argv.slice(2);
const flag = (name, dflt = null) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? args[i + 1] : dflt;
};
const has = (name) => args.includes(`--${name}`);
const num = (name, dflt) => Number(flag(name, dflt));

const URL_ = flag('url');
const IN = flag('in');
const OUT = flag('out', '.');
const NAME = flag('name', 'miku-window.m3u8');
const FILM_NAME = flag('film-name', null);
const MINUTES = num('minutes', 0);
const TAIL_BYTES = num('tail-bytes', 4 * 1024 * 1024);
const SEG_SECS = num('seg-secs', 2); // group this many seconds of fragments per segment
const READ_WINDOW = num('read-window', 64 * 1024); // bytes fetched per fragment step
const SAVE_PREFIX = flag('save-prefix', null); // optional: mirror the walked headers

const log = (...a) => console.log(...a);

// ---------------------------------------------------------------- fetching

let requests = 0;
let bytesRead = 0;

async function fetchRange(url, start, end, tries = 4) {
  let lastErr;
  for (let i = 0; i < tries; i++) {
    try {
      requests++;
      const res = await fetch(url, {
        headers: { Range: `bytes=${start}-${end}`, 'user-agent': 'fmp4-index/1' },
        cache: 'no-store',
      });
      const buf = Buffer.from(await res.arrayBuffer());
      if (res.status === 200) {
        if (buf.length < end + 1) throw new Error(`range ${start}-${end} -> 200 with ${buf.length} bytes`);
        bytesRead += end - start + 1;
        return buf.subarray(start, end + 1);
      }
      if (res.status !== 206) throw new Error(`range ${start}-${end} -> ${res.status}`);
      if (buf.length !== end - start + 1) throw new Error(`range ${start}-${end} -> ${buf.length} bytes`);
      bytesRead += buf.length;
      return buf;
    } catch (err) {
      lastErr = err;
      await new Promise((r) => setTimeout(r, 300 * (i + 1)));
    }
  }
  throw lastErr;
}

async function objectSize(url) {
  const res = await fetch(url, { method: 'HEAD', cache: 'no-store' });
  const len = res.headers.get('content-length');
  if (res.ok && len) return Number(len);
  const r2 = await fetch(url, { headers: { Range: 'bytes=0-0' }, cache: 'no-store' });
  const cr = r2.headers.get('content-range');
  if (cr) return Number(cr.split('/')[1]);
  throw new Error('could not determine object size');
}

// ---------------------------------------------------------------- box parse

function boxHeader(buf, off) {
  if (off + 8 > buf.length) return null;
  let size = buf.readUInt32BE(off);
  const type = buf.toString('latin1', off + 4, off + 8);
  let header = 8;
  if (size === 1) {
    if (off + 16 > buf.length) return null;
    const big = buf.readBigUInt64BE(off + 8);
    if (big > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error(`box ${type} too large`);
    size = Number(big);
    header = 16;
  } else if (size === 0) {
    size = buf.length - off;
  }
  return { type, size, header };
}

function parseMoov(buf, off, size) {
  const end = off + size;
  let timescale = null;
  let duration = null;
  const trackTimescales = new Map();
  const trex = new Map();
  let o = off + 8;
  while (o < end) {
    const h = boxHeader(buf, o);
    if (!h || h.size <= 0) break;
    if (h.type === 'mvhd') {
      const version = buf.readUInt8(o + 8);
      if (version === 1) {
        timescale = buf.readUInt32BE(o + 8 + 4 + 8 + 8);
        duration = Number(buf.readBigUInt64BE(o + 8 + 4 + 8 + 8 + 4));
      } else {
        timescale = buf.readUInt32BE(o + 8 + 4 + 4 + 4);
        duration = buf.readUInt32BE(o + 8 + 4 + 4 + 4 + 4);
      }
    } else if (h.type === 'trak') {
      let t = o + 8;
      const trakEnd = o + h.size;
      let trackId = null;
      let ts = null;
      while (t < trakEnd) {
        const th = boxHeader(buf, t);
        if (!th || th.size <= 0) break;
        if (th.type === 'tkhd') {
          const ver = buf.readUInt8(t + 8);
          trackId = buf.readUInt32BE(t + 8 + 4 + (ver === 1 ? 8 + 8 : 4 + 4));
        } else if (th.type === 'mdia') {
          let m = t + 8;
          const mdiaEnd = t + th.size;
          while (m < mdiaEnd) {
            const mh = boxHeader(buf, m);
            if (!mh || mh.size <= 0) break;
            if (mh.type === 'mdhd') {
              const ver = buf.readUInt8(m + 8);
              ts = ver === 1 ? buf.readUInt32BE(m + 8 + 4 + 8 + 8) : buf.readUInt32BE(m + 8 + 4 + 4 + 4);
            }
            m += mh.size;
          }
        }
        t += th.size;
      }
      if (trackId !== null && ts) trackTimescales.set(trackId, ts);
    } else if (h.type === 'mvex') {
      let x = o + 8;
      const mvexEnd = o + h.size;
      while (x < mvexEnd) {
        const xh = boxHeader(buf, x);
        if (!xh || xh.size <= 0) break;
        if (xh.type === 'trex') {
          const trackId = buf.readUInt32BE(x + 12);
          trex.set(trackId, { defaultSampleDuration: buf.readUInt32BE(x + 20) });
        }
        x += xh.size;
      }
    }
    o += h.size;
  }
  return { timescale, duration, trackTimescales, trex };
}

// One `moof` at `off`: total ticks per track, plus the earliest baseMediaDecodeTime.
function parseMoof(buf, off, size, trex) {
  const end = off + size;
  let o = off + 8;
  let baseTime = null;
  let dataOffset = null;
  const perTrack = [];
  while (o < end) {
    const h = boxHeader(buf, o);
    if (!h || h.size <= 0) break;
    if (h.type === 'traf') {
      let t = o + 8;
      const trafEnd = o + h.size;
      let trackId = null;
      let tfhdDefaultDuration = null;
      let tfdt = null;
      let ticks = 0;
      while (t < trafEnd) {
        const th = boxHeader(buf, t);
        if (!th || th.size <= 0) break;
        if (th.type === 'tfhd') {
          const flags = buf.readUInt32BE(t + 8) & 0xffffff;
          let q = t + 12;
          trackId = buf.readUInt32BE(q);
          q += 4;
          if (flags & 0x000001) q += 8;
          if (flags & 0x000002) q += 4;
          if (flags & 0x000008) {
            tfhdDefaultDuration = buf.readUInt32BE(q);
            q += 4;
          }
        } else if (th.type === 'tfdt') {
          const ver = buf.readUInt8(t + 8);
          tfdt = ver === 1 ? Number(buf.readBigUInt64BE(t + 12)) : buf.readUInt32BE(t + 12);
        } else if (th.type === 'trun') {
          const flags = buf.readUInt32BE(t + 8) & 0xffffff;
          let q = t + 12;
          const count = buf.readUInt32BE(q);
          q += 4;
          if (flags & 0x000001) {
            dataOffset = buf.readInt32BE(q);
            q += 4;
          }
          if (flags & 0x000004) q += 4;
          const hasDur = !!(flags & 0x000100);
          const hasSize = !!(flags & 0x000200);
          const hasFlags = !!(flags & 0x000400);
          const hasCto = !!(flags & 0x000800);
          const stride = (hasDur ? 4 : 0) + (hasSize ? 4 : 0) + (hasFlags ? 4 : 0) + (hasCto ? 4 : 0);
          for (let s = 0; s < count; s++) {
            const base = q + s * stride;
            let dur = tfhdDefaultDuration;
            if (hasDur) dur = buf.readUInt32BE(base);
            if (dur == null && trackId != null && trex.get(trackId)) {
              dur = trex.get(trackId).defaultSampleDuration;
            }
            if (dur == null) throw new Error('no sample duration (trun/tfhd/trex all silent)');
            ticks += dur;
          }
        }
        t += th.size;
      }
      if (baseTime === null || (tfdt !== null && tfdt < baseTime)) baseTime = tfdt;
      perTrack.push({ trackId, ticks });
    }
    o += h.size;
  }
  return { perTrack, baseTime, dataOffset };
}

// ---------------------------------------------------------------- walk

function newSegmentState() {
  return { segments: [], seconds: 0, lastBase: null, skipped: [], orphanMdats: [] };
}

function pushFragment(state, { start, end, seconds, baseTime }, timescales, fallbackTimescale) {
  let segSeconds = 0;
  for (const t of seconds.perTrack) {
    const ts = timescales.get(t.trackId) || fallbackTimescale;
    if (!ts) throw new Error(`no timescale for track ${t.trackId}`);
    segSeconds = Math.max(segSeconds, t.ticks / ts);
  }
  if (segSeconds <= 0) throw new Error(`zero-duration fragment at ${start}`);
  if (state.lastBase !== null && baseTime !== null && baseTime < state.lastBase) {
    throw new Error(`baseMediaDecodeTime went backwards at ${start}: ${baseTime} < ${state.lastBase}`);
  }
  state.lastBase = baseTime ?? state.lastBase;
  const prev = state.segments.at(-1);
  if (prev && prev.grouped < SEG_SECS - 1e-6 && prev.end === start) {
    prev.end = end;
    prev.seconds += segSeconds;
    prev.grouped += segSeconds;
  } else {
    state.segments.push({ start, end, seconds: segSeconds, baseTime, grouped: segSeconds });
  }
  state.seconds += segSeconds;
}

async function walkRemote(url, total, stopSecs, mirror) {
  requests = 0;
  bytesRead = 0;
  const started = Date.now();
  const state = newSegmentState();
  let initRange = null;
  let moovInfo = null;
  let moofBytes = 0;
  let moofCount = 0;

  const head = await fetchRange(url, 0, Math.min(READ_WINDOW, total) - 1);
  let o = 0;
  while (o < head.length) {
    const h = boxHeader(head, o);
    if (!h || h.size <= 0) break;
    if (h.type === 'ftyp') initRange = { start: o, end: null };
    else if (h.type === 'moov') {
      moovInfo = parseMoov(head, o, h.size);
      initRange.end = o + h.size;
      break;
    }
    o += h.size;
  }
  if (!initRange || initRange.end == null || !moovInfo) throw new Error('no ftyp+moov init segment at the head');
  log(
    `init: ftyp+moov = ${initRange.start}..${initRange.end - 1} (${initRange.end - initRange.start} B), ` +
      `timescale=${moovInfo.timescale}, mvhd duration=${moovInfo.duration} (${(moovInfo.duration / moovInfo.timescale / 3600).toFixed(2)} h), ` +
      `tracks=${[...moovInfo.trackTimescales.entries()].map(([k, v]) => `${k}@${v}`).join(',')}, trex=${moovInfo.trex.size}`,
  );

  let cursor = initRange.end;
  let lastLog = Date.now();
  while (cursor < total) {
    const want = Math.min(READ_WINDOW, total - cursor);
    const buf = await fetchRange(url, cursor, cursor + want - 1);
    const h = boxHeader(buf, 0);
    if (!h || h.size <= 0) {
      state.skipped.push(`unreadable box at ${cursor}`);
      break;
    }
    if (h.type === 'moof') {
      let moofBuf = buf;
      if (h.size > buf.length) moofBuf = await fetchRange(url, cursor, cursor + h.size - 1);
      const parsed = parseMoof(moofBuf, 0, h.size, moovInfo.trex);
      moofBytes += h.size;
      moofCount++;
      const moofEnd = cursor + h.size;
      let mdat = boxHeader(moofBuf, h.size);
      if (!mdat) {
        const b2 = await fetchRange(url, moofEnd, Math.min(moofEnd + 15, total - 1));
        mdat = boxHeader(b2, 0);
      }
      if (!mdat || mdat.type !== 'mdat') {
        state.skipped.push(`moof at ${cursor} followed by ${mdat ? mdat.type : 'EOF'}`);
        cursor = moofEnd;
        continue;
      }
      const end = moofEnd + mdat.size;
      if (end > total) {
        state.skipped.push(`last fragment at ${cursor} runs past the object`);
        break;
      }
      pushFragment(state, { start: cursor, end, seconds: parsed }, moovInfo.trackTimescales, moovInfo.timescale);
      if (mirror && state.segments.length && state.segments.at(-1).start === cursor) {
        // mirror the header bytes we actually read (a diagnostic artifact)
        writeSync(mirror, moofBuf.subarray(0, Math.min(h.size, moofBuf.length)));
      }
      cursor = end;
      if (state.seconds >= stopSecs) break;
    } else if (h.type === 'mdat') {
      state.orphanMdats.push({ start: cursor, size: h.size });
      cursor += h.size;
    } else {
      cursor += h.size;
    }
    if (Date.now() - lastLog > 15000) {
      lastLog = Date.now();
      const el = (Date.now() - started) / 1000;
      const rate = state.seconds / el;
      const remain = (stopSecs - state.seconds) / rate;
      log(
        `  … ${state.seconds.toFixed(0)}s of ${stopSecs}s walked, ${state.segments.length} segments, ` +
          `${requests} requests, ${(bytesRead / 2 ** 20).toFixed(2)} MiB read, ` +
          `${el.toFixed(0)}s elapsed, ~${(remain / 60).toFixed(1)} min left`,
      );
    }
  }

  for (let i = 0; i < state.segments.length; i++) {
    const s = state.segments[i];
    if (s.end > total) throw new Error(`segment ${i} extends past the object`);
    if (s.end <= s.start) throw new Error(`segment ${i} has no size`);
    if (i > 0 && s.start < state.segments[i - 1].end) throw new Error(`segment ${i} overlaps ${i - 1}`);
    if (s.seconds <= 0) throw new Error(`segment ${i} has zero duration`);
  }
  if (MINUTES > 0 && state.seconds + 1e-6 < MINUTES * 60) {
    throw new Error(`walked only ${state.seconds.toFixed(1)}s of the requested ${MINUTES * 60}s`);
  }
  const wall = (Date.now() - started) / 1000;
  return {
    ...state,
    initRange,
    moovInfo,
    truncated: false,
    stats: {
      wall,
      requests,
      bytesRead,
      moofCount,
      avgMoofBytes: moofCount ? moofBytes / moofCount : 0,
    },
  };
}

// Offline parse of a locally-held prefix (development / verification path).
function parseLocal(buf) {
  const state = newSegmentState();
  let initRange = null;
  let moovInfo = null;
  let o = 0;
  while (o < buf.length) {
    const h = boxHeader(buf, o);
    if (!h || h.size <= 0) break;
    if (h.type === 'ftyp') initRange = { start: o, end: null };
    else if (h.type === 'moov') {
      moovInfo = parseMoov(buf, o, h.size);
      initRange.end = o + h.size;
    } else if (h.type === 'mdat') {
      state.orphanMdats.push({ start: o, size: h.size });
    } else if (h.type === 'moof') {
      const next = boxHeader(buf, o + h.size);
      if (!next || next.type !== 'mdat') {
        state.skipped.push(`moof at ${o} followed by ${next ? next.type : 'EOF'}`);
        o += h.size;
        continue;
      }
      const end = o + h.size + next.size;
      if (end > buf.length) break; // truncated prefix: never emit half a fragment
      const parsed = parseMoof(buf, o, h.size, moovInfo.trex);
      pushFragment(state, { start: o, end, seconds: parsed }, moovInfo.trackTimescales, moovInfo.timescale);
      o = end;
      continue;
    }
    o += h.size;
  }
  if (!initRange || initRange.end == null) throw new Error('no ftyp+moov init segment found');
  return { ...state, initRange, moovInfo, truncated: false, stats: null };
}

// ---------------------------------------------------------------- playlist

function playlist({ segments, initRange, filmName }) {
  const target = Math.ceil(Number(Math.max(...segments.map((s) => s.seconds)).toFixed(3)));
  const lines = [
    '#EXTM3U',
    '#EXT-X-VERSION:7',
    `#EXT-X-TARGETDURATION:${target}`,
    '#EXT-X-MEDIA-SEQUENCE:0',
    '#EXT-X-PLAYLIST-TYPE:VOD',
    // No `#EXT-X-INDEPENDENT-SEGMENTS`: this file was not packaged for HLS, so
    // it is not claimed that every segment starts on a keyframe. A seek lands
    // on a segment boundary and the decoder may need until the next IDR to look
    // right; playback itself does not depend on it.
    `#EXT-X-MAP:URI="${filmName}",BYTERANGE="${initRange.end - initRange.start}@${initRange.start}"`,
  ];
  for (const s of segments) {
    lines.push(`#EXTINF:${s.seconds.toFixed(3)},`);
    lines.push(`#EXT-X-BYTERANGE:${s.end - s.start}@${s.start}`);
    lines.push(filmName);
  }
  lines.push('#EXT-X-ENDLIST');
  return lines.join('\n') + '\n';
}

// ---------------------------------------------------------------- modes

async function probe() {
  if (!URL_) throw new Error('--probe needs --url');
  const total = await objectSize(URL_);
  const start = total - TAIL_BYTES;
  log(`object size   : ${total} bytes`);
  log(`tail window   : ${start}..${total - 1} (${TAIL_BYTES} bytes)`);
  const buf = await fetchRange(URL_, start, total - 1);
  const found = [];
  for (const sig of ['mfra', 'tfra', 'sidx', 'ssix', 'moov', 'mfro', 'styp', 'moof', 'mdat']) {
    const hits = [];
    let i = buf.indexOf(sig, 0, 'latin1');
    while (i >= 0 && hits.length < 4) {
      hits.push(i);
      i = buf.indexOf(sig, i + 1, 'latin1');
    }
    if (hits.length) found.push(`${sig}@${hits.join(',')}`);
  }
  log(`box signatures: ${found.join('  ') || '(none)'}`);
  const indexed = ['mfra', 'tfra', 'sidx'].filter((s) => buf.includes(s, 0, 'latin1'));
  log(`verdict       : ${indexed.length ? `INDEX PRESENT (${indexed.join(',')})` : 'no random-access index in the tail'}`);
  return { total, indexed };
}

async function main() {
  let res;
  let total = null;
  let filmName = FILM_NAME;
  if (IN) {
    const buf = readFileSync(IN);
    log(`parsing local prefix: ${IN} (${buf.length} bytes)`);
    res = parseLocal(buf);
    if (!filmName) throw new Error('--in needs --film-name for the playlist URI');
  } else {
    if (!URL_) throw new Error('need --url or --in');
    total = await objectSize(URL_);
    log(`object size: ${total} bytes`);
    res = await walkRemote(URL_, total, MINUTES > 0 ? MINUTES * 60 : Infinity, SAVE_PREFIX ? openSync(SAVE_PREFIX, 'w') : null);
    // Keep the name exactly as it appears in the URL (percent-encoded): the
    // playlist is resolved against it by the browser, and an already-encoded
    // segment is passed through untouched.
    filmName = filmName || URL_.split('/').pop().split('?')[0];
    if (SAVE_PREFIX) closeSync(openSync(SAVE_PREFIX, 'a'));
  }

  const text = playlist({ ...res, filmName });
  await mkdir(OUT, { recursive: true });
  const file = path.join(OUT, NAME);
  await writeFile(file, text);
  const covered = res.segments.reduce((a, s) => a + (s.end - s.start), 0);
  log('');
  log(`init      : ${res.initRange.start}..${res.initRange.end - 1} (${res.initRange.end - res.initRange.start} bytes)`);
  log(`segments  : ${res.segments.length} (grouped at ${SEG_SECS}s)`);
  log(`duration  : ${res.seconds.toFixed(3)}s (${(res.seconds / 60).toFixed(2)} min)`);
  log(`avg seg   : ${(res.seconds / res.segments.length).toFixed(3)}s, ${(covered / res.segments.length / 2 ** 20).toFixed(2)} MiB`);
  log(`bytes     : ${(covered / 2 ** 30).toFixed(3)} GiB covered by the playlist`);
  log(`range     : ${res.segments[0].start} .. ${res.segments.at(-1).end - 1}`);
  if (res.stats) {
    log(
      `walk      : ${res.stats.wall.toFixed(1)}s, ${res.stats.requests} requests, ` +
        `${(res.stats.bytesRead / 2 ** 20).toFixed(2)} MiB read, ${res.stats.moofCount} moofs ` +
        `(avg ${res.stats.avgMoofBytes.toFixed(0)} B)`,
    );
  }
  log('first segments (offset, bytes, seconds):');
  for (const s of res.segments.slice(0, 3)) {
    log(`  ${String(s.start).padStart(12)}  ${String(s.end - s.start).padStart(9)}  ${s.seconds.toFixed(3)}`);
  }
  log('  ...');
  for (const s of res.segments.slice(-2)) {
    log(`  ${String(s.start).padStart(12)}  ${String(s.end - s.start).padStart(9)}  ${s.seconds.toFixed(3)}`);
  }
  if (res.orphanMdats.length) {
    log(`orphan    : ${res.orphanMdats.length} mdat with no moof before it: ${res.orphanMdats.map((x) => `${x.start}+${x.size}`).join(', ')} (not referenced)`);
  }
  if (res.skipped.length) log(`skipped   : ${res.skipped.slice(0, 6).join(' | ')}`);
  log(`wrote     : ${file}`);
  log('');
  log(text.split('\n').slice(0, 12).join('\n'));
}

if (has('probe')) await probe();
else await main();
