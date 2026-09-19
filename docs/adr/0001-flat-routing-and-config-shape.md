# Flat routing over a federated origin pool

D1 was blocked on R3 (Graph contract). The question was how a flat `GET /<key>` namespace maps to N OneDrive upstreams without leaking per-upstream layout into URLs.

We chose **server-side prefix routing**: `[[routes]]` is an ordered first-match table, sorted longest-prefix-first at load, with `prefix = ""` as the default. Routing decisions never change existing URLs — adding an upstream is a config-only change. Prefixes include their trailing `/` so `archive/` cannot accidentally match `archive2/`. Per-upstream `root_path` is the only provider-side path prefix; it is never interpolated from the key beyond appending the routed key. Alternative considered was per-upstream path prefix encoded in the URL (`/upstream/<key>`), rejected because it migrates every cached EdgeOne URL and leaks the pool topology to clients.

Config shape decision: `[[upstreams]]` carries `id`, `type`, `base_url`, `root_path`, the credential env pointers (`username_env`, `password_env`), an optional `cache_profile`, and the concurrency/routing knobs; `[[routes]]` carries `prefix` and `upstream` id. All secrets and the origin hostname (`ORIGIN_HOST`, TLS paths) are env indirection — the repo never holds literals. The canonical shape lives in `config.example.toml` and is mirrored in `docs/spec.md` §7.

*Amended 2026-09-18:* the upstream fields named here were the Microsoft Graph ones (`drive_root_path` and the three `*_env` OAuth pointers) from the upstream this project no longer runs. The routing half of this decision is unchanged and still in force; only the field list moved on, which is exactly what the `config.example.toml`-is-canonical rule is for.
