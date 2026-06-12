# MythDB — LSM Tree (Week 3A: MVCC Storage Core) — Design

**Date:** 2026-06-12
**Status:** Approved
**Reference:** [mini-lsm by skyzh](https://skyzh.github.io/mini-lsm/) chapters 3.1–3.2
**Builds on:** Week 1, Week 2A (compaction), Week 2B (persistence)

## Context

So far every key has exactly one value; an overwrite or delete replaces it. MVCC
(multi-version concurrency control) keeps every version of a key tagged with the
**commit timestamp** at which it was written, so a reader at timestamp `T` sees the
state as of `T`. Week 3A makes the storage engine multi-version; Week 3B (separate
spec) adds the transaction API, watermark/GC, and serializable snapshot isolation
on top.

### Decisions (from brainstorming)

- **Key encoding:** a stored key is `userKey || bigEndian(^ts)` (8-byte suffix).
  Because the timestamp is bit-complemented, plain `bytes.Compare` orders keys by
  user key ascending then timestamp **descending** (newest version first). This
  keeps the block/SST/memtable layers byte-oriented and almost unchanged.
- **Scope this sub-spec:** timestamped keys end to end, an MVCC iterator that reads
  at a given timestamp, monotonic commit timestamps assigned per write batch, and
  recovery of the latest timestamp. Compaction keeps **all** versions (GC is Week 3B).
- The existing public `Put`/`Get`/`Scan` API stays, reading at the latest committed
  timestamp.
- Style/testing: idiomatic Go, TDD.

## Goals

- A stored key carries a timestamp; multiple versions of the same user key coexist.
- `Get(userKey)` returns the newest version at the latest committed timestamp
  (tombstones read as absent).
- `Scan(lower, upper)` returns one entry per user key — the newest version visible
  at the read timestamp — skipping tombstones.
- An internal MVCC read at an arbitrary `readTs` returns the state as of `readTs`.
- Recovery restores the commit-timestamp counter so new writes never reuse a past ts.

## Non-Goals (Week 3A)

- Transactions, watermark, garbage collection, OCC, SSI (Week 3B).
- Dropping old versions during compaction (Week 3B watermark GC). 3A keeps all
  versions; storage grows on overwrite until 3B adds GC.

## Architecture

### Key encoding (`internal/key`)

```go
// Encode returns userKey || bigEndian(^ts). The complement makes bytes.Compare
// order equal user keys by timestamp descending (newest first).
func Encode(userKey []byte, ts uint64) []byte

// UserKey returns the user-key portion (everything but the 8-byte ts suffix).
func UserKey(encoded []byte) []byte

// Timestamp returns the decoded timestamp.
func Timestamp(encoded []byte) uint64

// Compare stays bytes.Compare: user key ascending, then ts descending.
func Compare(a, b []byte) int

// CompareUserKey compares only the user-key portions.
func CompareUserKey(a, b []byte) int

const (
	TsRangeBegin = ^uint64(0) // encode a lower scan bound: first (newest) version
	TsRangeEnd   = uint64(0)   // encode an upper scan bound / oldest version
)
```

All other packages call `key.Compare` on encoded keys, so their ordering logic is
unchanged. Lengths grow by 8 bytes (well within the u16 key-length fields).

### Lower layers (minimal change)

- **block:** unchanged. Stores encoded keys opaquely.
- **bloom / sstable:** the SST builder hashes `key.UserKey(encodedKey)` (not the
  whole encoded key) so a point lookup can ask "does this SST hold user key X"
  regardless of timestamp. `SsTable` gains a `maxTs uint64` field: the builder
  records the maximum timestamp seen and writes it into the footer; `Open` reads it.
  Footer becomes `[...][bloom][bloomOffset u32][maxTs u64]`.
- **memtable:** stores encoded keys. The engine looks up versions via range
  iterators (seek to `Encode(userKey, readTs)`), not exact `Get`.

### MVCC iterator (`internal/lsm`)

Replaces `lsmIterator`. Wraps the merged iterator over all tiers (which yields
encoded keys in user-asc / ts-desc order) and a `readTs`:

```go
type mvccIterator struct {
	inner   iterator.StorageIterator // merged tiers, encoded keys
	readTs  uint64
	upper   []byte // exclusive user-key upper bound; nil = unbounded
	curKey  []byte // current user key (decoded)
	curVal  []byte
	valid   bool
	err     error
}
```

Advancement (`moveToNextVisible`):
1. Skip every remaining version of the previously emitted user key.
2. For the next user key, skip versions with `ts > readTs`.
3. If a version with `ts ≤ readTs` exists, that (highest such) is the visible
   version. If its value is empty (tombstone), skip this user key and continue.
   Otherwise emit `UserKey` + value.
4. Respect the exclusive user-key upper bound.

`Key()` returns the decoded user key; `Value()` the value.

### Engine (`internal/lsm/storage.go`)

- `Storage` gains a commit-timestamp source. A small `mvcc` holder (mutex +
  `ts uint64`) provides `latestTs()` and `nextTs()` (increment and return).
- **Write(batch):** under `s.mu`, take `commitTs = s.mvcc.nextTs()`; for each op
  store `Encode(op.key, commitTs)` in the WAL and memtable. All ops in one batch
  share one timestamp (atomic in time).
- **Get(userKey):** `readTs = s.mvcc.latestTs()`; build a single-user-key MVCC scan
  and return the first entry (or not-found).
- **Scan(lower, upper):** `readTs = s.mvcc.latestTs()`; build the MVCC scan over the
  user-key range. Per-tier iterators seek to `Encode(lower, TsRangeBegin)` (or first)
  and the MVCC layer enforces the `upper` user-key bound.

### Recovery (`internal/lsm/recover.go`)

- WAL records already store the (now encoded) key, which embeds the timestamp.
  `RecoverWAL` replays them; the engine scans recovered keys for the maximum ts.
- Each opened SST contributes its `maxTs`. After recovery, set the commit-ts counter
  to the maximum timestamp observed across all WALs and SSTs, so the next write gets
  a strictly larger timestamp.

### Compaction (`internal/lsm/compact.go`)

3A keeps all versions: `doCompact` no longer drops tombstones or collapses keys; it
merges versioned entries and writes them through. Correctness of reads is preserved
by the MVCC iterator (newest visible version per user key, tombstones skipped). The
`toBottomLevel` parameter is retained but is a no-op in 3A (Week 3B uses it with the
watermark for GC).

## Data flow

- **Write:** assign one commit ts to the batch → encode each key with it → WAL +
  memtable → (freeze/flush as before, now storing encoded keys; flushed SST records
  its maxTs).
- **Read (Get/Scan):** read at the latest committed ts → merge tiers by encoded key
  → MVCC iterator yields the newest visible non-tombstone version per user key.
- **Recover:** rebuild structure (as Week 2B) → set commit-ts counter to the max ts
  found in WALs/SSTs.

## Error handling

- Unchanged from prior weeks: I/O and decode errors propagate; iterators surface
  errors via `Next`. Encoding/decoding a key is total (no error path) given a valid
  ≥8-byte encoded key; internal invariants guarantee that.

## Testing (TDD)

- `key`: `Encode`/`UserKey`/`Timestamp` round-trip; ordering — for one user key,
  higher ts sorts first; different user keys order by user key; range-bound encoding.
- `sstable`: bloom built on user key (a lookup by user key hits across timestamps);
  `maxTs` round-trips through Build/Open.
- `lsm` mvcc iterator: multiple versions of a key, read at several timestamps returns
  the right version; tombstone hides a key; dedup by user key; upper-bound respected.
- `lsm` engine: overwrite a key several times, reading at the latest ts returns the
  newest; existing Week 1/2 tests still pass (latest-read semantics); recovery sets
  the commit-ts counter so a post-recovery write outranks recovered versions;
  compaction retains versions (a key overwritten then compacted still reads newest).
- Full suite green with `go test ./...` and `go test -race ./...`.

## Build order (bottom-up, TDD)

1. `internal/key` — Encode/UserKey/Timestamp/CompareUserKey + constants.
2. `internal/sstable` — bloom on user key; `maxTs` in footer (Build + Open).
3. `internal/lsm` — `mvccIterator` (replacing `lsmIterator`), the commit-ts source,
   encoded writes, MVCC Get/Scan.
4. `internal/lsm/recover.go` + `compact.go` — restore commit ts on recovery; keep all
   versions in compaction.

## Open parameters

- Commit timestamps start at 1 on a fresh engine (ts 0 reserved as "before any
  write" for range-end encoding).
