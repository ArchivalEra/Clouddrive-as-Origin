# Metadata loss: degrade, and rebuild rows from the object tree

Decided while resolving the production-readiness map's crash-recovery ticket
(#57).

## What the measurement showed

The ticket assumed redb corruption panics at boot. Measuring it split the
claim in two — and the more dangerous half was not the one the ticket named.

Probed seven corruption shapes against a real store:

| Shape | Actual behaviour |
|---|---|
| Truncate to 0 / 1 KiB / half, zero the header, zero the whole file, random bytes throughout, garbage tail | **`open` succeeds.** `load_all` returns **0 rows**; `insert` works |
| Path is a directory / not a file | `open` returns `Err` |

So:

1. **Content corruption is silent, not fatal.** redb opens the file, the rows
   come back empty, and `load_and_start` fed that through
   `load_all().await.unwrap_or_default()`. The node started clean with zero
   entries while every cached file sat on disk orphaned — served by nothing
   (`serve_from_disk` needs a row), reaped by nothing (`reap_collect`
   iterates rows), reported by nothing. No log, no alert. That is worse than
   a crash, because a crash is visible.
2. **Unopenable metadata is fatal.** `MetaStore::open` propagated the error
   into `Cache::new`'s `.expect("open redb metadata store")`, and with
   `Restart=always` the node crash-looped.

The runbook's recovery step (`rm -f redb.db`) was also wrong about its own
effect: it claimed "cached files on disk are reused" when nothing rebuilt
the rows, so those files leaked.

## Decisions

1. **Unopenable metadata degrades instead of crashing.** The store logs an
   error, moves the bad file aside as `redb.db.corrupt-<epoch>`, and opens
   fresh. The cache is rebuildable metadata; losing it costs a revalidation,
   not correctness. A boot loop costs the whole service.
2. **Rows are rebuilt from the object tree.** When the store yields no rows
   but object files exist, `scan_object_files` walks the durable tree
   (top-level dot-entries are ephemeral artifacts, never objects; everything
   nested is an object) and recreates one entry per file. Rebuilt rows carry
   no ETag or mtime, so the first access takes the ordinary revalidation
   path and installs real metadata.
3. **Load failure is logged, not swallowed.** `unwrap_or_default()` is gone
   from that path; the error names itself before the rebuild runs.

The rebuild persists rows guard-free and installs them in one write stretch,
matching the C3 lock discipline (redb is never touched while the state guard
is held).

## Consequences

- A corrupt store costs revalidation traffic, not cached data and not uptime.
- The runbook's recovery step now describes what actually happens, and a
  boot-loop section documents the quarantine.
- `scan_object_files` is a full tree walk, so a rebuild on a very large cache
  costs one walk at startup. It only runs when the store came back empty,
  which is the exceptional case.

## Not decided here

Whether redb deserves scheduled backups. It remains rebuildable-in-principle
(rows are derivable from the tree), so a backup buys only avoided
revalidation; revisit if revalidation traffic after an incident proves
expensive.
