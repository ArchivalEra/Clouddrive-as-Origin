// A generic large-object reader, run inside a page served by the target origin
// (so `fetch` is same-origin and no CORS is involved).
//
// It is deliberately NOT media-aware: it reads byte ranges over HTTP the way any
// client of a large object does — sequentially with occasional jumps — and
// records what a viewer feels (the gap between successive bytes) plus what the
// request costs (time to first byte after a jump). Nothing here knows about
// containers, codecs or players; the origin under test serves any object.
//
// Installed as window.__readRange(opts) -> Promise<stats>.
window.__readRange = async function readRange(opts) {
  const {
    url,
    chunkBytes = 256 * 1024,   // bytes per request
    chunks = 64,               // sequential chunks before the first jump
    seeks = 4,                 // how many jumps to perform
    gapMs = 1500,              // a gap longer than this counts as a stall
    maxBytes = 64 * 1024 * 1024, // hard stop, so a run cannot outlive its welcome
    seed = 12345,
  } = opts;

  // A tiny deterministic PRNG: every viewer jumps to the SAME offsets, so the
  // comparison across viewers and runs is apples to apples.
  let state = seed >>> 0;
  const rnd = () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };

  let stats;
  window.__readerStats = stats = {
    requests: 0,
    bytes: 0,
    gaps: 0,          // gaps longer than gapMs
    gapTotalMs: 0,
    worstGapMs: 0,
    seekTtfbMs: [],
    checksum: 0,
    aborted: false,
    // The edge's own verdict per response (`eo-cache-status`), which is what
    // tells a "cold" run apart from a run that measured its own cache: a
    // re-run of the same offsets is a HIT and is not a cold reading.
    edgeHIT: 0,
    edgeMISS: 0,
    edgeOther: 0,
  };

  const size = Number(new URL(url).searchParams.get('size') || opts.size || 0);

  const beganAt = performance.now();
  let lastByteAt = beganAt;
  let read = 0;
  let pos = 0; // absolute object offset of the next byte, for the checksum

  async function get(start, len, isSeek) {
    pos = start;
    // Clamp to the object when its size is known: a reader that walks off the
    // end would otherwise collect 416s and call them errors.
    if (size) {
      if (start >= size) return;
      len = Math.min(len, size - start);
    }
    const began = performance.now();
    const res = await fetch(url, { headers: { Range: `bytes=${start}-${start + len - 1}` } });
    stats.requests++;
    if (!(res.status === 206 || res.status === 200)) {
      throw new Error(`range ${start}-${start + len - 1} -> ${res.status}`);
    }
    // Same-origin, so the response headers are readable: record what the edge
    // says it did rather than what we hope it did.
    const edge = res.headers.get('eo-cache-status');
    if (edge === 'HIT') stats.edgeHIT++;
    else if (edge === 'MISS') stats.edgeMISS++;
    else stats.edgeOther++;
    const reader = res.body.getReader();
    let got = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (got === 0) {
        const ttfb = performance.now() - began;
        if (isSeek) stats.seekTtfbMs.push(Math.round(ttfb));
      }
      const now = performance.now();
      const gap = now - lastByteAt;
      if (got > 0 && gap > gapMs) {
        stats.gaps++;
        stats.gapTotalMs += Math.round(gap);
        if (gap > stats.worstGapMs) stats.worstGapMs = Math.round(gap);
      }
      lastByteAt = now;
      got += value.length;
      stats.bytes += value.length;
      read += value.length;
      // A cheap rolling checksum over bytes sampled at FIXED OBJECT OFFSETS
      // (every 4 KiB), not at chunk positions: two viewers that read the same
      // ranges must produce the same number, whatever the network's chunking,
      // so this doubles as a cross-viewer equality check and a corruption check.
      const first = (4096 - (pos % 4096)) % 4096;
      for (let i = first; i < value.length; i += 4096) {
        stats.checksum = (stats.checksum * 31 + value[i]) >>> 0;
      }
      pos += value.length;
      if (stats.bytes >= maxBytes) {
        stats.aborted = true;
        await reader.cancel();
        return;
      }
    }
  }

  // Sequential read with jumps: the shape of a scrub, not of a download.
  const jumps = [];
  for (let s = 0; s < seeks; s++) jumps.push(s);

  try {
    let offset = Number(new URL(url).searchParams.get('start') || 0);
    await get(offset, chunkBytes, false);
    for (let c = 1; c < chunks; c++) {
      offset += chunkBytes;
      await get(offset, chunkBytes, false);
    }
    for (let s = 0; s < seeks; s++) {
      // Deterministic jump: somewhere else in the object, never backwards past 0.
      const span = Math.max(size - chunkBytes, 1);
      offset = Math.floor(rnd() * span);
      if (size && offset + chunkBytes > size) offset = size - chunkBytes;
      await get(offset, chunkBytes, true);
      for (let c = 1; c < Math.ceil(chunks / 4); c++) {
        offset += chunkBytes;
        await get(offset, chunkBytes, false);
      }
    }
  } catch (e) {
    stats.error = String(e && e.message ? e.message : e);
  }
  // Wall clock of this viewer's own reads. (The previous expression subtracted
  // a gap total from an absolute timestamp, which is not a duration.)
  stats.elapsedMs = Math.round(performance.now() - beganAt);
  stats.jumps = jumps.length;
  return stats;
};
