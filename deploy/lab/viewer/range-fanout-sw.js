// Range fan-out for a ready-made player, as a service worker.
//
// Measured on this project's leg: one serial connection sustains ~0.9 MB/s and
// a segment takes longer to fetch than it plays for, so the player stalls; four
// parallel requests measured 7.1 MB/s over the same leg. Off-the-shelf players
// (hls.js, dash.js) fetch one segment at a time and will not do that for you, so
// this layer does it for them: it intercepts each media Range request, splits it
// into `fanout` sub-ranges, starts them all at once, and hands the bytes back in
// order as one response. The player is untouched and never knows.
//
// Nothing here is media-aware: it is a byte-range multiplier for one URL
// pattern, and it works for any client that asks for ranges — hls.js, dash.js,
// a plain <video>, or the lab's own reader.
//
// Config comes from the script URL: range-fanout-sw.js?fanout=4&match=.m4s
self.FANOUT = 4;
self.MATCH = '.m4s';
self.MIN_PART = 256 * 1024; // a 1.4 KB init segment is not worth splitting

const cfg = new URL(self.location).searchParams;
const FANOUT = Math.max(1, Number(cfg.get('fanout') || self.FANOUT));
const MATCH = cfg.get('match') || self.MATCH;
const MIN_PART = Number(cfg.get('minpart') || self.MIN_PART);

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

function respondInParts(href, start, end, signal) {
  const total = end - start + 1;
  const parts = Math.min(FANOUT, Math.max(1, Math.floor(total / MIN_PART)));
  const chunk = Math.ceil(total / parts);
  const bounds = [];
  for (let s = start; s <= end && bounds.length < parts; s += chunk) {
    bounds.push([s, Math.min(s + chunk - 1, end)]);
  }
  const ctl = new AbortController();
  signal.addEventListener('abort', () => ctl.abort());
  // Every part starts now; the stream below only decides the order they are
  // handed back in. That is the entire trick.
  const pending = bounds.map(([s, e]) => fetchPart(href, s, e, ctl.signal));
  const body = new ReadableStream({
    async start(controller) {
      try {
        for (const p of pending) {
          const res = await p;
          const reader = res.body.getReader();
          for (;;) {
            const { done, value } = await reader.read();
            if (done) break;
            controller.enqueue(value);
          }
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
  return new Response(body, {
    status: 206,
    headers: {
      'Content-Type': 'video/mp4',
      'Content-Range': `bytes ${start}-${end}/*`,
      'Content-Length': String(total),
      'X-Fanout-Parts': String(parts),
    },
  });
}

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET' || FANOUT <= 1) return;
  const url = new URL(req.url);
  if (!url.pathname.includes(MATCH)) return;
  const m = /^bytes=(\d+)-(\d+)$/.exec((req.headers.get('range') || '').trim());
  if (!m) return; // no range, or an open-ended one: leave it to the network
  event.respondWith(respondInParts(url.href, Number(m[1]), Number(m[2]), req.signal));
});
