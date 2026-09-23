# The product ends at bytes: no client-side packaging

Settles who owns the viewer side when the content itself is not playable as
handed over — the case the real 200 GiB film finally produced. Companions:
`CONTEXT.md` "What this is", the runbook section "The real player on the real
film: the object is a fragmented MP4", and `deploy/README.md`'s viewer table.

## Context

The product criterion is a viewer scrubbing a 30-hour file smoothly, several
viewers at once, 30-200 GB. On the wire it is measured and met: 5 MiB shards at
7.06-7.1 MB/s with zero gaps and checksums verified, warm and cold multi-viewer
accounts on the real object, and a seek's first byte attributed to the provider's
`open` (806 ms) plus a 6 ms `stat` plus ~0 of ours — ADR-0016's "open is at its
floor" restated with numbers.

The last item on that list was a player that never started. It was chased down to
the content, not the path. The film is a **fragmented MP4**: `ftyp`, then a
2,436-byte sample-less `moov` carrying `mvex`+`trex`, then `moof`+`mdat` pairs of
~2 MiB with no `sidx` and no manifest. Chromium reads the init, finds nothing
playable, and abandons each request at a fragment boundary (the fragment walk:
15.6 MB of correct bytes received, then ~1.8 MiB steps, `readyState` 0 forever).
Beside it, in the same minutes: the page's own `fetch` with the same `Range`
moves 15.7 MB in 10 s, a bounded range completes in 26 ms, open-ended and bounded
responses at one offset are byte-identical, curl streams 40-41 MB per 60 s from
both vantages and both protocols, and the two-vantage comparison (workstation
direct, and through a tunnel to the node) produced the same 300-byte-shaped
walk from both. Edge, origin, vantage and protocol are exonerated by
measurement; the container is the answer.

Playing such an object means **a manifest plus MSE** — a player-side artifact.
That is the question this ADR settles: is that artifact part of this product?

## Decision

**The product's interface is bytes.** A flat, S3-compatible key space served by
ranged `GET`/`HEAD`, answered as fast as the deployment allows. What a client
does with those bytes is the client's program.

- No manifest or playlist generation, no container parsing, no transcoding, no
  media branch in what is served, no player page in the deliverable. This is not
  a new constraint — it is what "a 200 GiB video, a 100 GiB tarball, a VM image
  and a database dump are the same object to it" already required. This ADR
  applies it to the last item that was still listed as product work.
  The one extension-aware site, `src/mime.rs`, sets a response header for
  provider APIs that answer `application/octet-stream` (spec §3.9); it never
  selects which bytes are served, and it is not a precedent for reading the
  container.
- **Content preparation belongs to the content owner.** On the public web,
  adaptive streaming assumes the publisher produces HLS or DASH; every CDN makes
  that assumption, and a viewer page that hands an unindexed fMP4 to a bare
  `<video>` fails identically against any origin, CDN or not.
- **The client-side pieces under `deploy/lab/viewer/` are instruments and
  fixtures.** `player-page.html` with `range-fanout-sw.js`, and
  `package-for-player.sh`, exist so the wire can be measured with a real player
  and so an end-to-end demo is reproducible by whoever wants one. They are not
  deliverables, and nothing under `src/` may learn about them.

## What it costs

- The "open a page and watch the film" demo is incomplete without a client
  artifact this repository does not ship: someone must package the film (a
  single-file byte-range playlist, or a DASH manifest over the fragments) and
  host the page that plays it. That is a real cost and it is accepted; a customer
  who cannot do it needs a media server, which is a different product, not a
  change here.
- Acceptance for anything in this repository stays at the wire level — shards,
  gaps, checksums, seek TTFB, origin-side accounts — with the LAB's own
  small-object playback as the end-to-end smoke. "A real player played the real
  200 GiB film through the CDN" is a client-side demo, not a gate on an origin
  change.

## Evidence

- The atom walk and the three-armed wire dump: `deploy/lab/viewer/media-wire-dump.mjs`
  (complete request/response headers, `loadingFailed` reasons, per-request body
  bytes, connection and remote IP), read in the runbook section named above.
- The exonerations: page `fetch` vs media stack at one offset; open-ended vs
  bounded byte-identical; curl across two vantages and both protocols; LAB
  playback of a small object through the front (37 ms to metadata).
- The wire numbers the product is judged on: 7.06-7.1 MB/s in 5 MiB shards,
  zero gaps, checksums 3/3 and 4/4; 141 MiB warm and 120 MiB cold multi-viewer
  accounts; the two-hour sharded session (67 minutes, 0 errors).
- Where the boundary is enforced: `src/` has no media branch (hard constraint 1),
  `CONTEXT.md` states the object-agnostic contract, and the viewer table in
  `deploy/README.md` files every player-side piece as an instrument.
