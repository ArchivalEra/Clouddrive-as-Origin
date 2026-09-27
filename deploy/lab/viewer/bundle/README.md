# Viewer swarm — standalone bundle

Ten browser slots watch ONE remote video for five hours. Each slot runs
independent of the others: it starts at its own randomly chosen moment (never
ten at once), opens the video at a random position, plays five seconds, seeks
to another random position anywhere in the video, plays ten minutes, and dies.
The slot then refills with a new session, again at its own random moment. The
media is played by a plain `<video>` element — no special player, no plugins.

Every session writes one JSON line describing exactly what it did (startup,
the seek's timing, stalls, errors), so the run can be accounted for afterwards
without anyone watching it. The origin/video host is measured separately by
whoever operates it; your job is only the browser side.

## 0. What to run, in order

```sh
bash install.sh        # checks node / chromium / playwright-core / python3
bash preflight.sh      # ONE short session: proves the video plays and seeks
bash run.sh            # the five-hour run (parameters fixed inside; see below)
bash monitor.sh &      # in another terminal, BEFORE run.sh if possible
```

After the run finishes: `node report.mjs runs/sessions.jsonl > runs/report.txt`.

## 1. What you must be given

`MEDIA_URL` — the https URL of the video. Export it before `run.sh`:

```sh
export MEDIA_URL='https://...'
```

Nothing else. You do not need to know what the video is or who serves it.

## 2. The fixed shape (do not change it)

* ten slots, five hours;
* every session: play 5 s -> seek to a random position of the WHOLE video ->
  play 10 minutes -> die;
* slots start asynchronously: each draws its own uniform delay inside the first
  30 minutes, and refills inherit the desynchronization;
* refills carry a new random position and a 0-60 s jitter.

All of that is already inside `run.sh`. If you believe a parameter needs
changing, say so in your report instead of editing it — the run's value is that
its shape is the one that was agreed.

## 3. Environment

`install.sh` checks node (>= 18), chromium, playwright-core and python3, and
prints the install command for anything missing. The browsers are launched with
`--no-proxy-server`: the measurement must be direct, so the machine needs a
route to the media host without a proxy.

## 4. What to ship back

The whole `runs/` directory:

* `sessions.jsonl` — one JSON line per session (the account);
* `swarm.log` — the driver's progress lines;
* `monitor.log` — load / memory / session count every 5 minutes;
* `report.txt` — the summary produced by `report.mjs`. Its `bytes` line names
  how many sessions were counted on the wire (`counted on the wire in N/M`); if
  that reads `0/M`, the driver could not attach to the browser's network and the
  byte account is missing — say so in your report rather than shipping the run
  without it.

Also note, in one line each: any moment the machine was busy for another
reason, and whether the swarm had to be relaunched.

## 5. If something goes wrong

* The swarm dying (crash, power cut): relaunch `bash run.sh` with the same
  output directory — sessions append, and say so in your report.
* A slot's sessions all failing at startup: keep the run going and report it;
  startup failures are recorded per session, not guessed at.
* Do not "fix" failures by changing parameters mid-run.

## 6. Range size: what it does and does not change

Separate measurement, **not while the swarm is running** (it would pollute both):

```sh
MEDIA_URL='https://...' bash shard-sweep.sh            # 4 sizes x 5 samples
```

It answers a question that keeps coming back — "is a 2 MiB range slower than a
10 MiB one?" — where the honest answer is that **which number you read decides**.
Measured on this video from a client (2026-09-26, four sizes, 12 requests):

| range | TTFB | total | average rate |
|---|---|---|---|
| 2 MiB | 0.47–1.13 s | 2.86–3.69 s | 0.57–0.73 MB/s |
| 4 MiB | 0.46–0.94 s | 3.60–4.34 s | 0.97–1.16 MB/s |
| 5 MiB | 0.47–0.71 s | 3.57–5.13 s | 1.02–1.47 MB/s |
| 10 MiB | 0.48–0.72 s | 4.94–5.86 s | 1.79–2.12 MB/s |

Fit: **total ≈ 2.6–3.0 s + 0.24–0.26 s/MiB** (marginal ≈ 4 MB/s). So:

* **TTFB is flat across sizes** — if you measure "how long until the seek
  delivers", there is no difference to find, and that is not a broken
  measurement;
* **the average rate climbs with size because a per-request constant gets
  amortised**, not because big ranges transfer faster. Compare the slope, or
  total time, never the average rate;
* **this leg is noisy** (it has moved 300 B/s ↔ 7 MB/s minutes apart), so one
  sample per size decides nothing — the tool takes 5 and reports medians;
* all four sizes here came back `eo-cache-status: HIT`: the edge already holds
  that film, so this measures **client ↔ edge only**. The origin's own cost is a
  different number, measured on the origin host (`origin-side/`, run by whoever
  operates the origin): there, each cold read costs exactly **1 upstream open +
  1 stat** and pulls **exactly the bytes asked for** (2/4/5/10 MiB, no
  amplification), with a ~1 s first-byte constant — so a shard size changes
  **calls per byte**, not the transfer rate (a 2 MiB read pays one open per
  2 MiB, a 10 MiB read one per 10 MiB: five times fewer calls per byte).

For the product this is why the origin's counters are the authority and the
client's rates are only a symptom: the viewer's smoothness comes from reading
ahead and from the edge's cache, while the provider sees calls per window.

## 7. Signed links (presign.py) — for a player that must carry a ticket

The origin can require every content read to present a **SigV4 presigned URL**
(standard S3 query auth; ADR-0027). It is off in production until the rollout
flips it, but a player should be able to carry a ticket already, and a page
loaded from a signed URL needs no change at all: the ticket is just part of
the URL.

```sh
python3 presign.py --host <the host the browser requests> \
    --key googledrive1/<object> --session <per-viewer id> --expires 3600 \
    --id <access key id> --secret <secret>
# prints one URL; use it exactly where the plain object URL went
```

Rules that matter to a player:

* the **host you sign is the host the browser requests** (the signature covers
  the Host header), and the URL stops working when `--expires` passes — mint a
  fresh one per viewing session rather than sharing one;
* **GET and HEAD are separate tickets**: SigV4 signs the HTTP method, so a
  GET ticket refuses a HEAD probe. If the player probes with HEAD, ask the
  backend for `--method HEAD` URLs for those probes;
* `--session` is an opaque per-viewer id. It is signed (only whoever holds the
  secret can set it) and it is what the origin's budgets charge: a session that
  fires thousands of ranged reads a second is slowed with `503 SlowDown` after
  its first few, and recovers when its window slides. Treat a 503 as
  "back off and retry", never as a permanent failure;
* everything else is unchanged: ranged `GET`/`HEAD`, open-ended ranges, byte
  ranges of any size — the ticket does not constrain the read shape.

If a request comes back `403` with no signature complaint you can see, the
usual causes are a URL that has expired, a host that does not match what the
browser sends (a proxy or a rewritten domain), or a session marker that was
edited after signing — re-mint rather than patch the URL.

