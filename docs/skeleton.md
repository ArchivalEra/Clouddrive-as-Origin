# P1 — Skeleton proves the B-seam

Single binary, two planes in one `tokio` runtime:

- **Business plane** `127.0.0.1:8080` (`src/business.rs` — axum `GET /_internal/healthz`)
- **Front plane** `127.0.0.1:8443` (`src/main.rs` — plain-TCP reverse-proxy to business)

This is the cheapest artifact that can fail: it boots, proxies, and shuts down gracefully as one binary before we pull in Pingora (which will replace only the front's proxy with TLS/H2). See `deploy/origin-cache.service` for the systemd shape.

## Run

```sh
cargo run -- --help  # optional config path as argv[1]; defaults shown in config.example.toml
cargo run -- /path/to/config.toml

# In another shell:
curl -i http://127.0.0.1:8443/_internal/healthz  # via front
curl -i http://127.0.0.1:8080/_internal/healthz  # direct to business
```
