# The ticket endpoint

The site backend's minting half of ADR-0027, for a client that must not hold a
secret: a small worker that answers `POST` with one presigned URL per viewing
session. The blog client (REQ-mp-ticket-endpoint.md) implements the other half
and was tested against this exact contract before this file existed.

## The contract

- `POST`, same origin as the page. Body: `{"src": "https://<host>/<prefix>/<object>"}`
  — the raw, unsigned embed address.
- Checked: the caller's `Origin`/`Referer` must be the site origin; `src`'s host
  must be the hostname viewers type; the key must sit under the tenant prefix.
  Anything else is a bare non-2xx (the client falls back to the bare URL, so
  refusal bodies carry no detail).
- `200` → `{"url": "<presigned>", "expiresAt": "<ISO 8601>"}` with
  `Cache-Control: no-store`, and a session cookie on first success. Every later
  mint **derives its `session` from that cookie**: one cookie, one session id,
  forever — the origin charges its budgets per session, and a refresh that
  invents a new id hands the viewer a fresh budget.
- Tickets are 1 h (client renews before expiry and on playback errors); the
  deployment's ceiling is `front_content_auth_max_expiry_secs`.
- Logged: the session id, never a signed URL (the ticket is a credential).

## Deploy (the operator's three commands)

```sh
cp wrangler.toml.example wrangler.toml      # fill the REPLACE-ME values + the route
wrangler secret put TICKET_SECRET           # the site tenant's secret
wrangler deploy
```

The route binds it to the site's zone (same origin as the page, at the path the
client was configured with). The secret is a wrangler secret: the blog side can
ship the route and never holds the value, and rotation is `wrangler secret put`
plus nothing else (the worker reads it per request; no restart dance).

## What this worker must never become

- A proxy for bytes: media never flows through it — it only signs. That is why
  no CORS is needed and why its cost is one HMAC chain per request.
- A holder of anything else: no cookie jar, no allow-list, no per-viewer state
  beyond the rate valve. The origin's gate is the authority; this only mints.
- Configured with real values in this directory: the repo names no deployment
  (`wrangler.toml` with the route and the host is the operator's local file).
