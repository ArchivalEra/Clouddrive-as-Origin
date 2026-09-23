// Range fan-out for a player, as a service worker.
//
// Measured on this project's leg: one serial connection sustains ~0.9 MB/s and a
// segment takes longer to fetch than it plays for, so the player stalls; four
// parallel requests measured 7.1 MB/s over the same leg. Off-the-shelf players
// (hls.js, dash.js) fetch one segment at a time and will not do that for you, so
// this layer does it for them: it intercepts a media Range request, splits it
// into parallel sub-ranges and hands the bytes back in order as one response.
// The player is untouched and never knows.
//
// Two request shapes, because two kinds of client ask:
//   `bytes=A-B`  a segment — split once, parts in flight together (hls.js/dash.js)
//   `bytes=A-`   an open-ended read — a media element playing a plain MP4. Its
//                end is unknown, so the window rolls: always N parts in flight,
//                handed over in order, until the object ends.
//
// Nothing here is media-aware: it is a byte-range multiplier for one URL
// pattern, and it works for hls.js, dash.js, a plain <video>, or this repo's own
// reader.
//
// Config comes from the script URL: range-fanout-sw.js?fanout=4&match=.m4s
const cfg = new URL(self.location).searchParams;
const FANOUT = Math.max(1, Number(cfg.get('fanout') || 4));
const MATCH = cfg.get('match') || ''; // '' = every ranged request
const MIN_PART = Number(cfg.get('minpart') || 256 * 1024);
const PART = Number(cfg.get('part') || 4 * 1024 * 1024);

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));

async function fetchPart(href, start, end, signal, tries = 3) {
  let lastErr;
  for (let i = 0; i < tries; i++) {
    try {
      const res = await fetch(href, { headers: { Range: `bytes=${start}-${end}` }, signal });
      if (res.status !== 206 && res.status !== 200) throw new Error(`range ${start}-${end} -> ${res.status}`);
      return res;
    } catch (err) {
      lastErr = err;
      if (signal.aborted) throw err;
      await new Promise((r) => setTimeout(r, 300 * (i + 1)));
    }
  }
  throw lastErr;
}

const totalOf = (res) => {
  const m = /\/(\d+)\s*$/.exec(res.headers.get('content-range') || '');
  return m ? Number(m[1]) : null;
};

/// One pipeline for both shapes: `inFlight` is kept full, and parts are handed
/// to the client strictly in order — the bytes are the network's, the speed is
/// the parallelism's.
function pump(href, start, end, signal, parts) {
  const ctl = new AbortController();
  signal.addEventListener('abort', () => ctl.abort());
  const bounded = end !== null;
  let cursor = start;
  let total = null;
  const inFlight = [];
  const lastByte = () => (total !== null ? total - 1 : bounded ? end : Infinity);

  const launch = () => {
    if (cursor > lastByte()) return false;
    const s = cursor;
    const e = Math.min(s + PART - 1, lastByte());
    cursor = e + 1;
    inFlight.push({ s, e, p: fetchPart(href, s, e, ctl.signal) });
    return true;
  };

  const body = new ReadableStream({
    async start(controller) {
      try {
        for (let i = 0; i < parts; i++) if (!launch()) break;
        while (inFlight.length) {
          const { s, e, p } = inFlight.shift();
          const res = await p;
          if (total === null) total = totalOf(res);
          const reader = res.body.getReader();
          let got = 0;
          for (;;) {
            const { done, value } = await reader.read();
            if (done) break;
            got += value.length;
            controller.enqueue(value);
          }
          if (got < e - s + 1) cursor = Infinity; // short read: the object ended
          launch();
        }
        controller.close();
      } catch (err) {
        controller.error(err);
      }
    },
    cancel() {
      ctl.abort();
    },
  });
  const headers = { 'Content-Type': 'video/mp4', 'X-Fanout-Parts': String(parts) };
  if (bounded) {
    headers['Content-Range'] = `bytes ${start}-${end}/*`;
    headers['Content-Length'] = String(end - start + 1);
  }
  return { body, headers, size: () => total };
}

async function respond(href, start, end, signal) {
  const bounded = end !== null;
  const parts = bounded
    ? Math.min(FANOUT, Math.max(1, Math.floor((end - start + 1) / MIN_PART)))
    : FANOUT;
  const { body, headers } = pump(href, start, end, signal, parts);
  if (bounded) return new Response(body, { status: 206, headers });
  // An open-ended request has no last-byte-pos to report, and a media element
  // needs one (a 200 would tell it "the file starts here", which breaks seeks).
  // One 1-byte probe answers the question, so the response is a normal 206.
  const probe = await fetchPart(href, 0, 0, signal, 2);
  const total = totalOf(probe);
  if (total === null) return new Response(body, { status: 206, headers });
  return new Response(body, {
    status: 206,
    headers: { ...headers, 'Content-Range': `bytes ${start}-${total - 1}/${total}`, 'Content-Length': String(total - start) },
  });
}

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET' || FANOUT <= 1) return;
  const url = new URL(req.url);
  if (MATCH && !url.pathname.includes(MATCH)) return;
  const m = /^bytes=(\d+)-(\d*)$/.exec((req.headers.get('range') || '').trim());
  if (!m) return;
  event.respondWith(respond(url.href, Number(m[1]), m[2] ? Number(m[2]) : null, req.signal));
});
