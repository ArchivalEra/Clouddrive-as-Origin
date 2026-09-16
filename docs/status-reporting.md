# Status reporting to the blog worker

The node reports its own health to the blog-side status card and alerting
channel. Interface agreed 2026-09-16 with the blog-side agent; this file is
the contract both sides implement.

## Transport and authentication

```
node watchdog ──▶ https://api.mango-mesa.ccwu.cc/api/activity/origin-cache
                  (cloudflared tunnel → Cloudflare Worker)
```

- The request leaves the node over the **existing `cloudflared` tunnel**
  (`/etc/systemd/system/cloudflared.service`,
  `tunnel run --token-file /etc/cloudflared/token`). The tunnel is
  outbound-only: the node opens no inbound listener for this.
- **The tunnel token is the only credential.** No `Authorization` header, no
  query parameter, no shared secret on this side — so the watchdog holds no
  secret and needs no `EnvironmentFile`.
- The receiving end answers `200` with
  `{"ok":true,"id":"origin-cache-origin-1",...}`.

## Cadence and the dead-man switch

- The sender is `origin-cache-watchdog.timer` (**every 5 minutes**), not the
  service itself. This is deliberate: a detector has to be independent of the
  thing it detects. A report sent by the origin-cache process would go silent
  exactly when that process dies.
- The far side declares the node **offline after 15 minutes of silence**
  (three missed beats).
- **That timeout is the real outage detector.** A node that is powered off,
  out of network, or dead at kernel level cannot report anything at all —
  not even the `down` event below. The `down` event only makes a *process*
  death earlier and more informative than 15 minutes of silence.

## Payloads

Heartbeat (every run, healthy or not — a degraded node still has to prove it
is alive):

```json
{
  "schema": "origin-cache/status/1",
  "host": "origin-1",
  "event": "heartbeat",
  "ts": "2026-09-16T13:54:01Z",
  "service": "origin-cache",
  "version": "0.1.0",
  "status": "active",
  "entries": 13050,
  "bytes": 592000000
}
```

`status` is one of:

| value | meaning |
|---|---|
| `active` | every unit active, both healthz probes answered, no disk verdict |
| `degraded` | the node serves, but something needs a human: the nocache plane is down, the disk crossed a watermark, or a healthz probe reported its own degraded verdict |
| `down` | the standard plane is not serving (unit inactive, or its healthz probe failed) |

Down (non-clean exit only):

```json
{
  "schema": "origin-cache/status/1",
  "host": "origin-1",
  "event": "down",
  "ts": "2026-09-16T13:54:26Z",
  "service": "origin-cache-standard",
  "status": "down",
  "death": {
    "signal": "SIGKILL",
    "exit_code": 137,
    "reason": "systemd service result=signal code=killed exit=137"
  }
}
```

`death` is built from systemd's `$SERVICE_RESULT` / `$EXIT_CODE` /
`$EXIT_STATUS`, so it says *why* the process left: `signal` is the signal name
when the kernel killed it (`SIGKILL` here means a kill -9, or an OOM kill),
`exit_code` is numeric (128 + signal, or the process's own exit status), and
`reason` is free text for a human. `signal` and `exit_code` are `null` when
they do not apply.

Note that `service` names the **unit** in a down event (`origin-cache-standard`
or `origin-cache-nocache`) and is the literal `origin-cache` in a heartbeat.

## Rules the sender follows

1. **A planned stop is silent.** systemd marks a requested stop with
   `SERVICE_RESULT=success`, and the hook returns without reporting. Every
   deploy is a stop-start and `Restart=always` restarts on its own, so
   reporting those would turn each deploy into a false outage alert.
2. **Down reports are throttled to one per minute** (`notify-down.stamp`).
   `Restart=always` restarts after 3s, so a crash loop would otherwise fire
   the hook ~20 times a minute and defeat the far side's timestamp dedupe.
   The heartbeat carries `status=down` meanwhile.
3. **No retries.** `post()` has a 5s ceiling, and a failure only writes a
   `report ... failed http=NNN` line to `watchdog.log`. The next heartbeat
   covers the gap; a reporting failure must never affect serving or the
   health verdict.
4. **Only the documented events and fields are sent.** No raw healthz body is
   forwarded: `store.moved_to` carries a filesystem path
   (`/opt/origin-cache/...`) and `upstreams[].id` carries the upstream's name,
   and neither should leave the node.

## Open items on this interface

- The agreed schema has **no field for `degraded_reasons`**, so a `degraded`
  heartbeat says that something needs attention but not what. Adding a field
  is the far side's call; until then read the reasons on the node:
  `curl -s http://127.0.0.1:8080/_internal/healthz | jq .degraded_reasons`.
- There is **no `recovered` event** in the agreed schema; recovery is implied
  by the next heartbeat with a healthy `status`.

## Verifying on the node

```sh
# Send one heartbeat right now and read the answer
sudo -u opc /opt/origin-cache/watchdog.sh

# Confirm the units carry the ExecStopPost hook
systemctl cat origin-cache-standard | grep ExecStopPost

# Confirm the tunnel the report rides is up
systemctl is-active cloudflared

# The sender's own record of failures and of down reports
grep -E 'report |down report' /opt/origin-cache/watchdog.log
```

`watchdog.log` stays quiet on purpose (verdict transitions, one `HB` line a
day, plus report failures), so an empty grep is the healthy case.