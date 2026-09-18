# Stopping: exit at once when idle, and what the drain actually protects

Decided while closing the code-side gap to production. Two things are
recorded here because they arrived together and one corrects a belief the
other was resting on.

## The problem

Pingora's graceful path stops the listener the instant SIGTERM arrives and
then sleeps its grace period unconditionally — 300s by default, no early exit
for an idle server. Measured on the oracle node, every deploy's stop took
305–309s, i.e. about five minutes of an origin serving nothing. The units'
`TimeoutStopSec=320s` exists to cover that sleep; before it, systemd's 90s
default SIGKILLed every stop, so graceful shutdown never ran at all.

## Decision 1: the stop is adaptive

`src/shutdown.rs` samples the front plane's in-flight connection gauge
(`origin_front::CONNECTIONS_ACTIVE`) across a 3s settle window:

- **never leaves zero → exit immediately** (`FastExit`), skipping Pingora's
  sleep;
- **anything in flight → keep the full drain** (`Drain`), and a non-zero
  sample ends the window early so a busy node starts draining at once.

The settle window covers a connection accepted in the instant before the
listener closed (the gauge increments early in a connection's life, not at
the socket) and the access-clock flusher's one-second cadence, so pending LRU
timestamps land before the exit.

**Measured:** an idle stop went from 305s to **2s** (`Result=success`,
`ExecMainStatus=0`, and `exiting without the drain window` in the journal).
A busy stop still took **305s** and logged `connections in flight; draining
gracefully`.

**Accepted consequence:** the fast path skips the business plane's drain.
With no front connections there is no client work in flight, only loopback
health probes, which their client treats as a miss either way.

## Decision 2: the drain does not promise a transfer survives, and we now say so

The 300s was assumed to exist so in-flight transfers could finish. It does
not do that, and that was measured rather than argued.

Test: fetch a 100 MiB object with the client throttled to 1 MB/s, restart the
service mid-transfer, compare with the same fetch and no restart.

| run | client received | front's own log |
|---|---|---|
| restart mid-transfer | 100,663,243 of 104,857,600 (broken pipe) | `status=200 bytes=104857600` — the session completed |
| no restart (control) | 104,857,600 — complete | `status=200 bytes=104857600` |

So the drain does a great deal — the client got 98s of a 100s transfer
instead of stopping at the ~7s mark — but the **tail is lost**: about 4 MiB
of the 100 MiB, roughly the last four seconds at that rate. The front had
written the whole body into the session and closed; the bytes still in flight
never arrived. It is not a matter of the grace period being too short: the
session had already ended, so more sleep would change nothing.

**What is therefore true, and what we stopped claiming:**

- A deploy is not free for a slow client. It truncates the tail of whatever
  is in flight, and a client with a retry resumes from where it stopped.
- Shrinking the grace period would cost much more than the tail (the transfer
  would stop at the SIGTERM mark), so the busy path keeps it.
- The mechanism that discards the tail — session close versus a socket-level
  linger — is **not identified**. That needs a look at Pingora's session
  lifecycle, and it is recorded as a candidate ticket rather than guessed at
  here.

## Consequences

- Idle deploys are seconds; busy deploys are still ~305s, and the runbook says
  so.
- `SERVICE_RESULT=timeout` remains a real failure: the budget (320s) has been
  measured to be sufficient for the drain, so exceeding it means something is
  wrong rather than that the stop was ordinary.
- The `ExecStopPost` hook still reports every non-clean exit, so the change
  does not affect death reporting.
