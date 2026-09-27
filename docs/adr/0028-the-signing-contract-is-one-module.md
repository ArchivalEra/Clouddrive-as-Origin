# The signing contract is one module, and the testable claims are fixtures

Settles where the signed-read protocol lives (ADR-0027's mechanism) and how its
four statements are kept from drifting. Written the day `docs/signing.md` took a
session to write and the cross-checking it forced found three real gaps: a
verifier that did not require the host header to be signed, a shipped signer
that could mint a URL the gate always refuses, and a documented SDK recipe that
quietly used the wrong signer class. Companions: ADR-0027 (the mechanism),
`src/signing.rs` (the module), `tests/signing_contract.rs` (the checks),
`docs/signing.md` (the reader-facing half).

## Context

One protocol was stated in six places: 35 refusal strings in the verifier, 12
deny sites in the gate, `deploy/oracle/presign.py`, a shell helper in the LAB
suite, a ten-row table in `docs/signing.md`, and a copy of that README in a
bundle outside the repo. Nothing tied them together, so:

- the signer's `--session` was optional while the gate required it: the tool
  could hand out tickets the door always rejects;
- the "cross-language pin" in the unit tests was a frozen signature string —
  editing the tool turned nothing red, and the only live cross-check lived in a
  suite CI does not run;
- a new refusal could ship undocumented, and a metric's `reason` label set was
  "bounded" by comment rather than by construction;
- one config knob had five homes and eight sites, and the content-auth knobs had
  no boot validation at all (a zero silently refused every signed read).

## Decision

**The contract is a module (`src/signing.rs`), and the claims that can be
checked are checked.**

- `Reason` is a closed enum with an exhaustive `as_str` (a new variant is a
  compile error until it is named), a `status` (403, or the S3-standard 503 for
  the two budget refusals), `ALL` (the whole taxonomy) and `documented()` (an
  exhaustive decision: which refusals a caller can act on). The metric's
  `reason` label set is `ALL` by construction.
- The caps live in the same module (`Caps` + `DEFAULT_*` + `PROTOCOL_MAX_...`),
  with one `validate()`; config's default functions read the same constants the
  gate enforces, and `Config::from_raw` refuses a zero or an over-protocol cap
  at boot.
- `param::*` names the query parameters in production code. Fixtures keep their
  literals on purpose: a fixture spelled out *is* the wire-format pin.
- `tests/signing_contract.rs` runs the shipped signer as a subprocess against
  the shipped gate (built through the real `Config` and credential loader), and
  asserts the documentation table against the taxonomy in both directions —
  every documented reason present, no stale row, and the status column equal to
  `Reason::status()`.
- The documentation may keep prose the code cannot express (what a refusal
  means, how to fix it). It may not keep a *list* that can drift.

## What it costs

- Two spellings collapsed into one label each (the header and query forms of
  "no usable signature", "missing credential", the two "credential scope date"
  phrasings). One log line reads slightly differently than before; every string
  a test or the document pinned is byte-identical.
- `presign.py` is stricter than a generic SDK: it refuses to mint without
  `--session`, and `--no-session` exists to reproduce the refusal. A caller who
  wanted a sessionless URL was always going to be refused at the door.
- The signature-conformance half of the new suite needs `python3`; it
  print-and-skips where the tool is absent (the house rule for external tools,
  `tests/watchdog_script.rs`), and CI runs an explicit `python3 --version` step
  so that half of the gate cannot disappear quietly there.
- `documented()` is a judgement call per refusal, reviewed like any other. The
  alternative — documenting all 33 — buries the eleven rows that change what a
  caller does.

## Evidence

Reverse-validated both ways on the day it landed: injecting a protocol drift
into `presign.py` (`UNSIGNED-PAYLOAD` → a typo) turns 7 of the 9 contract tests
red; the documentation test failed on its first run against the real document
(one cell names two reasons); renaming a documented knob in
`config.example.toml` turns the completeness test red; a zero in any of the
three content knobs is refused at boot. Ladder at the time: 361 tests, clippy
`-D warnings` 0, LAB `--smoke` 45/0 with the signing section exercising the tool
through the live gate.
