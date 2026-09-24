# Pitfalls

The traps this repository has actually fallen into, one entry each, with the
evidence that put it here. They are the most durable thing in the project: every
other document explains a design, and this one explains what the design cost.

It lives in the repository because it used to live only in a gitignored working
note, which is the wrong place for the knowledge you want after an incident. Add
to it whenever something costs more than ten minutes to find; do not remove an
entry for being obvious in hindsight — hindsight is the point.

## Working on the repo

1. **`pkill -f <pattern>` kills the shell that runs it** when the pattern appears
   anywhere else in the same command line — and a heredoc in that command line
   counts. The `[x]` bracket trick does not save you from that case. Five
   incidents in one session. *Fix:* put cleanup in a script
   (`deploy/lab/*.sh`, `/tmp/clean-lab.sh`) and let the caller's command line
   mention only the script's path; or use `pgrep -f 'x[y]' | xargs kill`, which
   cannot match itself.

2. **The `.gitignore` is a whitelist.** Everything is ignored unless a `!` line
   un-ignores it. A new root-level file needs `!/name`, and `#` at the start of
   that line makes it a comment — the `git add` then skips the file silently and
   the whole commit fails. *Fix:* after committing, `git log --oneline -1` to
   confirm a commit actually appeared.

3. **A TOML key is owned by the table above it.** Appending a key after the first
   table header puts it in that table, where serde ignores it unless the struct
   denies unknown fields — which is how `session_window_bytes` under a
   `[cache_profiles.x]` table and several keys after `[[routes]]` were silently
   dropped. *Fix (done):* every config struct now carries
   `#[serde(deny_unknown_fields)]`, so this is a boot error that names the key;
   `config.example.toml` and every `deploy/**/*.toml` are parsed by
   `every_shipped_config_parses_strictly` so strictness cannot surprise a deploy.

4. **`A && B && C & sleep N` backgrounds the whole chain.** Waiting N seconds and
   reading the log then shows the PREVIOUS run's file. *Fix:* launch long jobs
   with `setsid nohup <script> > log 2>&1 &` as one command, then poll separately.

5. **A shell helper that matches only numbers reads a string field as empty.**
   `hz_field <port> status` printed nothing for `"status":"ok"` and the assertion
   that used it failed for a reason nobody could see in the output.
   *Fix:* grep the field's real shape (`grep -q '"status":"ok"'`).

6. **`local a=$1 b=$((...a...))` cannot see `a`.** Under `set -u` the subshell
   dies without a trace: the symptom was "0 requests, wall 3 ms", which looks
   like a logic bug. *Fix:* one variable per `local` line.

7. **In Rust, a partially moved value cannot be dropped**, and private fields
   cannot be destructured from an integration test. `let body = served.plan.body`
   then `drop(served)` does not compile; destructuring `Served { plan, lease,
   watch }` does not either when the guards are private (on purpose — `protect`
   is the only way to obtain them). *Fix:* borrow the field
   (`collect(&mut served.plan.body)`) and drop the whole response.

## The cache's own semantics

8. **Sealing is asynchronous.** The driver renames the span and merges the ledger
   AFTER the body's last byte reaches the viewer, so sampling staged state the
   instant a response returns races it. *Fix:* poll (`wait_ledger`,
   `wait_segment_bytes`, `wait_spans`, `wait_staged`, `wait_settled`).

9. **Adjacent `.seg` intervals do not merge** — `add_interval` merges overlapping
   intervals only, deliberately, so each keeps its own read clock (window decay).
   A scan of touching spans therefore produces one interval per span. *Fix:* a
   test whose premise is a "merged ledger" must build that row by hand; a scan
   cannot produce it.

10. **Do not measure an open delta around a single request.** Since ADR-0018 the
    chain fetches the NEXT window while a key is watched, and that open can land
    inside the measurement window. *Fix:* `settle_opens` (two equal samples, up to
    12 s) before taking the baseline.

11. **A pause only proves protection if it outlives two clocks.** The span-level
    min-age guard is 60 s and the reaper ticks every 60 s, so a "pause" shorter
    than ~120 s cannot demonstrate that a watch, a lease or a grace period was
    what saved the bytes. *Fix:* pause longer, or set `read_grace_secs = 0` and
    the idle TTL small to isolate the mechanism under test.

12. **Efficient staging assumes admission can afford the WINDOW.** A magazine
    smaller than the window (or than the object the walk stages into) refuses the
    write and the request goes passthrough — every assertion about staged bytes
    then reads zero and passes vacuously. *Fix:* a LAB magazine must hold the
    window, and the assert should name the span it expects.

13. **A test double that lies about its promise breaks the pump.** A mock that
    reported `promised_len = Some(7)` while serving 8 bytes was harmless until the
    pump started reading AT MOST the promise — then it silently truncated bodies.
    *Fix:* doubles promise what they deliver; the one that lies on purpose
    (`overlong`) is the case the cap exists for.

14. **A test that never ran looks exactly like a test that passed.** A function
    that had lost its `#[tokio::test]` compiled, warned, and was counted as
    coverage for a whole admission rule. *Fix:* `cargo build` warning-free is part
    of the gate, and a named test is only evidence once it has been seen to fail.

## Measurement

15. **Never trust a path you have not measured.** Three concurrent requests from
    a workstation to the CDN stalled one connection for 8.7 s, 11.9 s and once
    over 300 s, while the origin's own counters never moved; the same three
    requests from the node answered in 0.21 s. A "concurrent viewers through the
    CDN" number taken from that workstation measures the workstation.
    *Fix:* `deploy/lab/probe-edgeone-viewers.sh` runs the account from the node.

16. **The LAB's timing assertions need a quiet box.** The same revision produced 7
    FAILs at load 32-46 (extra spans, half spans, a refused connection) and 60
    PASS / 0 FAIL at load 4 — a false red that sends somebody hunting a
    regression that is not there. *Fix:* the suite refuses to start above
    `MAX_LOAD` (default 6) or with orphan browsers alive; `MAX_LOAD=<n>` overrides.

17. **Orphan browsers outlive a killed harness.** A `timeout`-killed
    `multi-viewer.mjs` leaves chromium processes that keep the box busy for
    minutes and are invisible in a casual `ps`. *Fix:* check
    `pgrep -c 'chromi[um]'` (the bracket keeps the check from matching itself)
    before blaming the code.

18. **A profile's `waiting` event before the first `playing` is startup
    buffering**, not a stall; counting it fails every run. `readyState === 0`
    (HAVE_NOTHING) is the honest signal that playback never began. *Fix:* count
    stalls only after the first `playing`.

19. **Media requests are invisible to page JavaScript.** Resource timing gives
    their sizes without the request; the real shape (offsets, order, retries) is
    only in CDP (`Network.enable` → `requestWillBeSent`). *Fix:*
    `deploy/lab/viewer/player-probe.mjs`.

20. **A probe's range header is the closed interval** `bytes=first-first+len-1`.
    Writing `first-len` gets a 416, and a 416 can be misread as "0 ms seek" if the
    output is not checked. *Fix:* assert the status code, always.

21. **Checksums across viewers must sample by OBJECT offset**, not by chunk
    position: each viewer's chunk boundaries differ, so position-based sums differ
    for correct bytes.

22. **`ls` without `-a` cannot see `.seg.*`** (leading dot), so an assertion about
    sidecars silently reads zero. *Fix:* `ls -a`, or count through the store's own
    API.

## Environment

23. **An ssh timeout kills the remote process it started.** A long node-side
    experiment dies with the session. *Fix:* `setsid nohup bash /home/opc/<x>.sh
    > log 2>&1 < /dev/null &` in one ssh, then poll the log in another.

24. **`sudo systemctl` needs `-n` on the node** (otherwise: "Interactive
    authentication required"), and a node rebuild must not run `install.sh` unless
    the configs or units changed too. *Fix:* the recipe in `docs/runbook.md`.

25. **`cargo test` / `cargo build` must not overlap a LAB run**: the timing
    assertions flake, and one real failure was once amplified into 31 false ones.

26. **Chromium cannot fetch a page through a CSP that blocks inline scripts**, so
    a harness that injects `<script>` fails on a real origin. *Fix:* evaluate the
    reader source in the page context (`page.evaluate`), which is not subject to
    CSP — `deploy/lab/viewer/` does exactly that.

27. **An open-ended `Range: bytes=N-` is what a browser player sends first**, and
    answering it literally on a 200 GiB object means promising 200 GiB
    (`content-range: bytes 0-214748364799/…`). It is tempting to blame the shape
    for the player that then never starts. Measured 2026-09-21, it is not the
    shape: the edge relays the promise and returns correct bytes, rewriting the
    player's requests into bounded windows changes nothing, and the real cause was
    the object (a ~5-second clip padded with 0xFF to exactly 200 GiB, no index, so
    a player must scan 200 GiB for a duration). *Fix:* measure the object before
    the transport — `fake-total-server.py` changes only the advertised total, and
    `cdn-wire-probe.mjs` says who ended each request.

28. **A bare `wait` waits for the long-lived process the script itself started.**
    A probe that launches an instance in the background (`&`) and then fires its
    requests in parallel cannot use `wait` to join them: it also waits for the
    instance, so the script never returns and the ssh channel stays open (the
    first rate-limit probe hung exactly there, after printing pass lines into a
    log nobody could read). *Fix:* collect the request PIDs and `wait <pid>` each,
    or keep the instance and the join in separate steps.

29. **Every request a suite or probe makes must be bounded.** One stalled request
    hung the whole LAB with no output at all — the log stopped mid-section and sat
    there until the wrapper's 1600 s ceiling — twice, before the shared `H` helper
    grew `--connect-timeout 5 --max-time 120`. A failure you can read beats a hang
    you cannot: bound the request, then assert on the status.

30. **A "200 GiB test object" is not a long video until its bytes say so.** The
    object this project tested players against is 200 GiB exactly, and its real
    content is a two-fragment 5-second clip; everything past ~31 MB is 0xFF
    padding. Every conclusion about "a huge object plays badly" drawn from it is
    really a conclusion about an unindexed clip plus filler. *Fix:* before
    blaming a path, a CDN or a range shape for a player that will not start, look
    at the object (`ffprobe` its head, `od` a few offsets, `fake-total-server.py`
    to change only the advertised total) and at who ended the requests
    (`cdn-wire-probe.mjs`).

31. **A `pkill -f <pattern>` kills the process whose command line contains the
    pattern — including the shell that is running the command.** Pitfall #1 in a
    new costume: the pattern was inside a heredoc in the same command, so the
    tool's own shell matched and died mid-script, printing nothing at all. *Fix:*
    put the killer in a script file (`bash /tmp/x.sh`) so no live command line
    contains the pattern, or kill by port (`fuser -k 7811/tcp`) or by recorded
    PID.

32. **A `git push` that hangs eats the whole retry loop.** The remote here goes
    through a proxy that occasionally just stops answering: one attempt sat in
    state `S` for 15 minutes producing nothing, so a loop of five unbounded
    attempts would have burned an hour. *Fix:* bound every attempt
    (`timeout 100 git push origin HEAD`) inside the loop, and check the exit code
    rather than the tail of the output; the next attempt then runs on a fresh
    connection and usually succeeds first try.

33. **Record where each end of a CDN path actually IS before drawing a
    verdict.** The same hostname gave this workstation a POP in Singapore
    (43.174.246/247.108, AS139341) and the origin node also a POP in Singapore,
    while the origin itself is in Phoenix, Arizona — so a Chinese viewer's bytes
    cross the border twice and the measured 0.4-1.4 MB/s says nothing about the
    origin's code. Three checks, each one command: the leg's POP
    (`curl -w '%{remote_ip}'`), the client's own egress (`myip.ipip.net`), and
    what the audience's resolvers return (`dig @223.5.5.5 <host>`). *Fix:* record
    the geography, and then measure the shape you promise before reading a
    verdict out of it: the same domestic client through the same overseas POP
    moved 7.1 MB/s in 5 MB shards, so "the POP is overseas" is a fact, not a
    verdict.

34. **A proxy can hide which vantage you are measuring — and this one exits from
    the origin host.** `http_proxy`/`ALL_PROXY` here point at 127.0.0.1:2080,
    whose egress is the origin node itself; a CDN measurement
    taken through it would look like the origin talking to the edge. *Fix:* use
    `--noproxy '*'` (or `--no-proxy-server` for a browser) AND prove it — curl
    prints `Established connection to <host> (<pop-ip>) from <local-ip>` when it
    goes direct, and prints `Uses proxy env variable` when it does not.

35. **A per-request latency is not a rate.** A cold 1 MiB read from the origin
    took 1.1-1.2 s, which reads as "0.9 MB/s, the upstream is the cap" — until a
    cold 64 MiB read of the same object took 3.3 s, i.e. 20 MB/s: the 1.1 s was
    the upstream OPEN's latency, paid once per stream, not a throughput ceiling.
    *Fix:* measure throughput with a span big enough to amortise the open (tens of
    MiB), and measure latency separately, before naming either one the bottleneck.
    The same trap sat in the other direction all night: the CDN's 0.4-1.4 MB/s
    "ceiling" was one bad moment on one leg, not a property of the path.

36. **The request shape is part of the measurement, not a detail of it.** One
    open-ended `Range: bytes=N-` on the 200 GiB film gave 300 B to 1.4 KB/s and
    a player that never starts; four concurrent readers asking in 5 MB shards on
    the SAME object, client, route and night gave 7.1 MB/s with zero gaps and
    correct checksums — four orders of magnitude. Two weeks of this project's
    own tests had already settled it (shards in, single huge pull out), and a
    day of this session was spent re-learning it with a player probe. *Fix:*
    name the shape in every CDN/route/origin verdict, and take the verdict with
    `multi-viewer.mjs --chunk-bytes 5242880 --viewers N` (plus `--unique-seeds`
    when the viewers should not share a fill).

37. **Cold bytes through an edge are a shared budget, not a per-client rate.**
    The same edge that served one client 7 MB/s also pulls cold data at a median
    of 265 ms per 1 MiB fill request (about 4 MB/s while filling, 0.33 MB/s
    averaged over a mixed 7-minute window) — and that supply is divided among
    every viewer who wants bytes the edge does not have yet, while cache hits go
    out at line rate. *Fix:* state which one a number is. "Zero gaps for one
    viewer on 5 MB shards" and "six viewers, 390 MB of distinct cold ranges" are
    different experiments with different answers.

38. **A smaller read-ahead window is not an optimization until the boundary hands
    over.** Cutting the floor from a whole window (64 MiB) to 8 MiB looked like a
    pure win — a 5 MiB jump stages 8 MiB instead of 64 — and the LAB measured it
    as a LOSS: a 24 MiB ascending shard walk cost **8 opens** (one per three
    shards) against 1, because every request that landed exactly at a live
    window's end escaped to its own Range while it waited for that window to
    seal. It only pays once `Sessions::start` takes the key over at the boundary
    (ADR-0024). *Fix:* when a change makes a boundary more frequent, measure the
    boundary, not the steady state — a walk is a sequence of boundaries.

39. **A binary's architecture and libc are part of the deploy.** A local x86_64
    build copied to the aarch64 node fails at `EXEC` (`Exec format error`) and
    leaves both units in `activating` — three or four minutes of downtime,
    measured 2026-09-22, made worse by running `install.sh`, which the deploy
    recipe says is for config/unit changes only. The subtler half is the libc:
    the compile machine's `aarch64-linux-gnu-gcc` builds fine and produces a
    binary the node refuses with `version 'GLIBC_2.38' not found`, because the
    toolchain is Debian trixie (glibc 2.43) and the node is Oracle Linux 9.8
    (2.34). *Fix:* `file` the artifact, run it once with a bogus config path
    (`Error: load config` proves it executes), keep a copy of the running binary
    for rollback, and prefer a STATIC toolchain — musl removes the version
    question entirely instead of pinning an answer to it.

40. **Appending a query with `?` to a URL that already has one silently corrupts
    every parameter after it.** The cold-band flag was wired by rebuilding the
    viewer URL as `...?size=${size}&start=${band}`, but the module-level URL
    already ended in `?size=...`, so the request became
    `?size=214748364800?size=214748364800&start=...`: `Number("214748364800?size=214748364800")`
    is NaN, the reader's object size became NaN, and every jump offset became
    NaN (`range NaN-NaN -> 416`, measured). *Fix:* append with `&` when the URL
    carries a query, or build the query with `URL`/`URLSearchParams` and set
    fields on it.

41. **A "cold" reading has to carry its own proof.** `--unique-seeds` picks
    *different* offsets from fixed constants, so the second run of the same
    command re-reads what the first one warmed: a warm reading wearing a cold
    label. *Fix:* `multi-viewer.mjs --cold-band` gives every viewer a fresh band
    and a fresh jump seed per run, and the reader records the CDN's own
    `eo-cache-status` per response — the report's `edgeHIT`/`edgeMISS` columns are
    what make "cold" a fact rather than a claim.

42. **A reading can outlive the object it was taken on.** ADR-0023 was written
    around "the provider `stat` costs 108 ms", measured on the synthetic
    `round3.mp4`; on the real 200 GiB film the same metric is **5.8 ms** (273
    calls; 1 ms inside a three-jump window) against an 824 ms `open` — so the
    proposal had nothing to win, and implementing it would have bought a
    version-gate design for six milliseconds. *Fix:* before building on a
    reading, re-take it on the object the decision is about, and record both.

43. **More upstream concurrency cannot exceed the pipe.** `concurrency_per_upstream`
    is the obvious suspect when a CDN path is slow, so it was raised 3 -> 12 and
    the same cold multi-viewer run repeated: the origin's per-ask service time was
    unchanged (p50 225 vs 223 ms), its ask count did not rise, and nothing
    improved — because the constraint was never the gate but the link: 16 MiB in
    one Range from the node took 26.6 s and 12.9 s (0.63-1.31 MB/s) with TTFB at
    0.21 s, i.e. the origin answers immediately and the bytes then crawl, and a
    cold run's *fill* crosses that same link. *Fix:* when a path is slow, measure
    the pipe end to end before turning a concurrency knob — and remember that a
    shard shape can move 5-10x what one large Range does over the same link.

44. **A control-plane call travels this workstation's proxy too.** `HTTP_PROXY` /
    `HTTPS_PROXY=http://127.0.0.1:2080` are exported globally, so a `tccli` call
    goes through the same proxy every measurement here must avoid — and it fails
    quietly, as a network error against a hostname that looks unrelated to the
    proxy. *Fix:* `unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy` in the
    shell that makes the call. Prove it with a black-hole proxy: point
    `HTTP_PROXY` at a closed port, and a call that still reaches the API (a real
    `requestId` comes back) is a call that did not use the proxy — with the
    variables set, the same call dies on the proxy instead.

45. **The right key against the wrong endpoint reads as a wrong key.** EdgeOne
    International is served by `teo.intl.tencentcloudapi.com`; tccli has no
    international routing at all and defaults to the domestic endpoint, which
    answers `AuthFailure.SecretIdNotFound`. That is an error about the credential
    for a problem that is the endpoint, and it sends you to regenerate a key that
    was fine. *Fix:* pass `--endpoint teo.intl.tencentcloudapi.com` explicitly,
    and do not believe what an auth error says about the key until the endpoint
    is known to be right.

46. **A read response is not a write request.** EdgeOne spells the same setting
    differently on the two sides — the read returns `ZoneSetting.UpstreamHttp2`,
    the write takes `ZoneConfig.UpstreamHTTP2` — so round-tripping a read into a
    write silently *drops* settings instead of erroring, which is the failure
    mode that costs an afternoon and a serving domain. *Fix:* send only the block
    you intend to change, then re-read and diff the whole setting object; any
    field that moved unasked-for means stop and read the diff, not carry on.

47. **An hourly CDN metric can put a whole burst in the wrong bucket.** The
    origin-pull timing metric reads about 15% above the origin's own body bytes
    and smears a burst across a bucket boundary: one hour reported **49.61 MB** of
    origin-to-edge traffic while the origin's own access log shows **zero pulls**
    in that hour. A "viewerless pull" found from a one-hour bucket is an artifact,
    and it is convincing enough to invent a `CachePrefresh` problem out of
    nothing. *Fix:* answer origin-pull questions from the origin's per-request
    access log, and use the CDN metric for totals only.

48. **`xff=-` in the front log does not mean "the edge".** The front logs every
    request it serves, and `X-Forwarded-For` is simply absent for the ones that
    arrive directly at the node: our own `accept.sh` contract checks (400 on
    reserved names, 404 on nested look-alikes, HEAD, `/favicon.ico`) and the LAB
    probes never leave the box. Counting those as edge pulls turns a few KB of
    self-checks into a phantom background pull, and reading `xff` correctly is what
    separates "the edge asked for this" from "we asked ourselves". *Fix:* split on
    `xff=` before attributing anything to the edge -- a value there is the client as
    EdgeOne saw it, `-` is a direct hit.

49. **A `spawn_blocking` round trip inside a response body's poll stalls the
    drain.** Sealing runs at the tail of the body's own stream
    (`ranged::upstream_body`), so a `tokio::fs::metadata(..).await` there — a
    blocking-pool hop from inside a `Stream` — left the body undrained:
    `a_read_credits_every_staged_span_it_touches` failed with `flight stalled: no
    progress for 30s` in ~35% of runs, and 0% once the same `stat` was made
    synchronous. The house style in this area is already `std::fs::metadata`
    (the sweep, the version-change cleanup, the disk-covers check). *Fix:* when a
    future hangs in a body path, look for an async filesystem call first, and
    prefer the synchronous one where the file is page-cached anyway.

50. **A test that samples after a response races the seal.** The seal lands when
    the body's tail runs or the disconnect watcher fires — after the response the
    test is holding. `a_read_credits_every_staged_span_it_touches` asserted the
    ledger straight after a request and failed **4 of 6 runs on the tree as it
    stood before this was noticed**; the file already had `wait_ledger` for
    exactly this, used by ten other tests. *Fix:* wait for the record, not for
    the response — and when a test is flaky, check the baseline first (this one
    was not the change's fault).
51. **A union is not a substitute for the question.** Two protections answer
    differently — a lease shelters a whole key, a watch shelters a neighbourhood —
    and one word covering both ("spared") made the budget stop enforcing itself on
    watched keys the moment a new module read the union as the budget's answer:
    `without_a_pin_the_policy_takes_the_oldest_span` caught it. The old comment
    (`pin_of`: "the callers fall back to sparing the whole key when there is no
    pin") described one caller and not the other, and nothing in the code said
    which was which. *Fix:* give each question its own name — `spared` for the
    union, `verdict` (`{ leased, pin }`) for the budget — and let the compiler
    keep callers from picking the wrong one.

52. **A refusal that has to be kept in sync is not a refusal.** The front refused
    the business plane's private surface by naming `/_internal/healthz` exactly,
    in a crate that does not depend on the plane: a rename on either side
    silently republished the route, and every internal route added later would
    have to be remembered. *Fix:* guard the PREFIX (`/_internal/` except the one
    documented public entry), so growing or renaming a route cannot open it. The
    same shape as a whitelist that must be updated in two places: prefer the rule
    that needs no news.
53. **A scripted edit that PRINTS success may have changed nothing.** Adding
    `--progress-secs` to `multi-viewer.mjs` with a python `str.replace` whose
    anchor silently did not match left the file untouched, while the script
    printed "added" — so the two-hour session ran **completely silent**, with no
    way to see that anything was wrong until its summary two hours later. The
    same pattern (edit by `replace`, no assertion, trust the print) is used all
    over this project's tooling. *Fix:* `assert anchor in text` before every
    replacement, and `grep` the changed file afterwards. A harness that reports
    nothing looks exactly like a harness that is working.

54. **A long run has to report while it runs.** Both viewer harnesses printed only
    at the end (per-viewer rows and a total), which is fine for the LAB's
    thirty-second runs and useless for a session measured in hours: a run that
    died at minute ten would look identical to one that is working. *Fix:*
    `--progress-secs N` on both (`multi-viewer.mjs`, `player-probe.mjs`), reading
    the page's own live counters (`reader.js` publishes `window.__readerStats`),
    with `<- NOT PLAYING` on the probe so a player that never starts says so.

55. **A "trial run" of the binary is only safe when it cannot reach the live
    cache.** Handing a valid-but-wrong config (one mis-typed env var name) to the
    production binary opened the live metadata store *before* the admission check
    fired: the running unit held the lock, so the store layer quarantined
    `redb.db` aside and created an empty one. Nothing looked wrong — the running
    unit kept serving from its still-open inode — and the next restart would have
    come up on an empty database. *Fix:* every rehearsal gets its own `cache_dir`
    and its own ports (the checks now also run before the store is touched, but
    the habit is the fix); `install.sh`'s env-file rehearsal, the deploy recipe's
    fake-config run and anything that starts the binary share this rule. If it
    does happen: `mv -f <quarantined> redb.db` restores the name, and the running
    unit's fd already points at the real inode.

56. **Dropping a response body does NOT stop the transfer behind it — so
    "seal on viewer disconnect" cannot be exercised through the public
    interface.** Measured 2026-09-23 with a paced upstream (1 KiB per 100 ms) and
    a 64 KiB ranged passthrough: after the viewer's body was dropped, the
    `.segpart` kept growing (1024 → 19456 bytes over the next two seconds), i.e.
    the writer outlives its reader and seals at the window's own tail. The
    watcher's comment ("viewer disconnect drops the whole body stream, so the
    seal code below never runs on abort") describes a case this path does not
    produce; its numbers (600 polls of 100 ms, three stable polls) remain
    unpinned, and the planned move into `cache::staging` was DROPPED rather than
    done unverified. *Fix before touching it:* find a case where the writer
    really stops mid-window (the driver cancelled, not the reader), or leave the
    watcher alone.

57. **Measure from the client, not from the origin node.** Every "the CDN is
    fast/slow" conclusion drawn from a shell on the origin is a conclusion about
    the origin's own loopback: the edge is not in that path at all. The retired
    `deploy/verify-cold-pull.sh` existed because four earlier conclusions were
    wrong for exactly this reason. *Fix:* run CDN measurements from a real client
    vantage (or the LAB's browsers), and keep the node for what it can answer —
    the origin's side of the account.

58. **Assert the bytes, not the speed.** A truncated transfer is fast, and a
    curl that gives up at `--max-time` looks like a completed one in a byte
    count nobody printed. Every cold-pull conclusion has to come with
    `size_download == expected` (or a checksum), and a distinct failure for
    timeout (exit 28) versus truncation (exit 18).

59. **Know which cache you measured.** A hit at the edge says nothing about the
    origin, and an origin hit says nothing about the edge: the two have separate
    caches, separate keys and their own clocks. The account has to name the layer
    (`eo-cache-status` for the edge, the origin's own counters or front-access
    log for the pull) — pitfall 5 in measurement clothing.

60. **A conclusion needs two runs that differ only in the thing you claim.**
    Letting the network vary under a measurement makes every later comparison
    meaningless: the same shape, minutes apart, moved 300 B/s and 7 MB/s (measured
    2026-09-22). *Fix:* repeat, and change one variable at a time — the shard
    harness's `--unique-seeds` versus a fixed seed is the pattern.

61. **The LAB's player row (§15) can fail on a busy machine with unchanged code.**
    Measured 2026-09-23, twice, same binary: the first `--quick` run (started while
    `load` was 3.1, minutes after a release build and the full test suite) reported
    `FAIL: a real player stalled` — the media element waited at t=13.2 s and the
    fourth request's *body* took 30,118 ms to deliver 1.3 MB over loopback
    (`maxGapBetweenRequests=30419ms`). The second run on a quiet machine (load 1.3)
    was 75/0 with `stalls=0` and `maxGapBetweenRequests=90ms`. The row is a
    wall-clock judgement about a real browser decoding video, so the machine's
    state is part of the measurement. *Fix:* read `uptime` before the run and
    re-run before believing a single §15 failure — but if it recurs on a quiet
    machine, treat it as a lead, not as noise: compare the body duration against
    the object size (1.3 MB in 30 s is four orders off, not a slow disk).

62. **A browser-side fixture that works against a local server can hang against
    the CDN.** `deploy/lab/viewer/range-fanout-sw.js` (the service worker that
    splits a media range into N parallel sub-ranges — the "glue" that measured
    7.1 MB/s against a local 900 KB/s server) never delivers a body byte when
    the same page is served from the CDN: the synthesized response arrives in
    ~4 ms with `Content-Range: bytes A-B/*` and `Content-Length` set, and then
    nothing — for a 1 KiB range and for a 4 MB range alike, 30 s timeouts in
    both cases, while the identical fetch without the worker streams fine. The
    worker's own `fetch()` never settles. **Nothing is concluded about the
    cause** (recursion, a Chromium quirk with worker-initiated ranged fetches,
    or the CDN's HTTP/2 handling are all open); what is concluded is that a
    local-server measurement of a client-side fixture does not transfer to the
    CDN, and that the 2026-09-24 run had to go without it. *Fix before relying
    on it:* reproduce it with the worker's own `fetch` instrumented, and treat
    "measured against log-server.py" as unproven for any remote target.

63. **Ten Chromium contexts on an 8-core / 7 GiB box put it at load ~30, and
    then the client-side failures are not the origin's.** Measured over 4.5
    hours: 32 of 279 viewer sessions failed, 23 of them `page.goto` timing out
    at 60 s **on an 8 KB page**, and 8 hls.js `fragLoadTimeOut` — while the
    origin's own counters showed zero 5xx and a healthy store throughout. Ten
    contexts (their renderers, decoders, network and audio threads) plus
    continuous rebuffering is simply more than 8 cores want; the box swaps
    (22 GB of swap free, 0.3-1 GB of RAM available) and the scheduler delays
    everything the harness does. *Fix:* either accept it and report the load
    next to every client-side number, or shrink the swarm — but do not read a
    workstation failure as a product one.

64. **An fMP4's own headers are a cheap index; its payloads are not.** Building
    an HLS byte-range playlist for the 200 GiB film needed no re-encode and no
    download of the object: each `moof` states its size and carries its samples'
    durations, and the following `mdat` states its size, so a walk can hop from
    fragment to fragment reading ~600 bytes each. 1811 ranged reads and 113 MiB
    of bytes covered 3.2 GiB (30 minutes, 899 segments) — 3.5% of the window's
    bytes. The corollary is what makes it worth remembering: the *edge* still
    pulls a 1 MiB shard per little read, so an index walk over the WHOLE object
    would pull ~half of 200 GiB from the provider (~2.8 h at the measured
    ~20 MB/s). Index a prefix, not a film.

65. **`/tmp` is tmpfs: a long run's data dies with the machine.** The first
    attempt at the 2026-09-24 swarm (ten viewers, 4.5 h) started at 17:13Z and
    was ~1.5 h in with 50+ sessions when a power cut took the machine — and the
    sessions file, the driver log and the index walk's output went with it,
    because all of it had been written to `/tmp`. The fixtures survived only
    because they were in the bucket (Google Drive) and the tools only because
    the repo is on the HDD. *Fix:* write anything that takes hours — sessions
    JSONL, progress logs, monitor lines, baselines — under `/mnt/hdd`, and treat
    `/tmp` as the scratch space it is.

66. **A test that passes on a tmpfs `/tmp` has not passed.** The suite had been
    green for months on a machine whose `/tmp` is RAM, and the first CI run on a
    runner with a real disk went red: `cargo test --lib` failed
    `business::tests::a_partially_covered_range_needs_one_open_and_stages_the_rest`
    (242 passed, 1 failed), twice. The seal's ledger accounting lags the response
    it belongs to — nanoseconds on a tmpfs, milliseconds to tens of milliseconds
    on a disk — so three sites that asserted immediately after a response were
    racing, and the tmpfs had been hiding it. Reproduced locally in **one** run
    with `TMPDIR=/mnt/hdd/tmp cargo test`; two more sites surfaced under that
    (the fully-covered-seek test's stage-to-seek step, and the random
    consistency walk's per-round account-equals-disk assert, 3 of 6 runs).
    *Fix:* wait for the RECORD, never assert on the response (pitfalls 50/51) —
    `wait_ledger` before span asserts, and in the consistency walk wait until
    the account and the disk agree; the budget invariant also needs
    tick, settle, re-tick, because a span that seals after a tick is over budget
    until the next one. *Rule:* run the suite once with `TMPDIR` pointed at a
    real disk (CI does that implicitly), and treat "green locally" as evidence
    about the filesystem, not about the code.
