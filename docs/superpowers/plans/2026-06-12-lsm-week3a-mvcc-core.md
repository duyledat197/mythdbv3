# LSM Tree Week 3A (MVCC Storage Core) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the engine multi-version: keys carry a timestamp, writes get monotonic commit timestamps, and reads return the newest version visible at a read timestamp.

**Architecture:** A stored key is `userKey || bigEndian(^ts)`, so `bytes.Compare` orders by user key ascending then timestamp descending. Lower layers stay byte-oriented. A new `mvccIterator` reads the merged tiers at a read timestamp, yielding one visible non-tombstone version per user key. Recovery restores the commit-ts counter.

**Tech Stack:** Go 1.26, standard library only. Builds on Weeks 1–2B.

**Spec:** `docs/superpowers/specs/2026-06-12-lsm-week3a-mvcc-core-design.md`.

**Conventions:**
- Module `mythdb`. Run from repo root. Commit after each task; use `git -c user.name='Claude' -c user.email='noreply@anthropic.com' commit` if needed.
- After each task run `go test ./...` AND `go vet ./...`; confirm green before committing.

---

## File Structure

```
internal/
  key/key.go                  (modify) Encode/UserKey/Timestamp/CompareUserKey + constants
  sstable/builder.go          (modify) bloom on user key; record maxTs; write maxTs in footer
  sstable/sstable.go          (modify) read maxTs from footer; MaxTs()
  lsm/mvcc.go                 (new)    commit-timestamp source
  lsm/mvcc_iterator.go        (new)    mvccIterator (read at a timestamp)
  lsm/iterator.go             (modify) remove lsmIterator (replaced); keep fusedIterator
  lsm/storage.go              (modify) mvcc field; encoded Write; MVCC Get/Scan
  lsm/recover.go              (modify) restore commit ts from WAL/SST max timestamps
  lsm/compact.go              (modify) keep all versions (no tombstone drop in 3A)
```

---

## Task 1: Timestamped key encoding

**Files:**
- Modify: `internal/key/key.go`
- Test: `internal/key/encode_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/key/encode_test.go`:
```go
package key

import "testing"

func TestEncodeRoundTrip(t *testing.T) {
	enc := Encode([]byte("hello"), 42)
	if string(UserKey(enc)) != "hello" {
		t.Fatalf("user key = %q", UserKey(enc))
	}
	if Timestamp(enc) != 42 {
		t.Fatalf("ts = %d", Timestamp(enc))
	}
}

func TestOrderingNewestFirstSameUserKey(t *testing.T) {
	// Same user key: higher timestamp must sort BEFORE lower timestamp.
	a := Encode([]byte("k"), 10)
	b := Encode([]byte("k"), 5)
	if Compare(a, b) >= 0 {
		t.Fatalf("expected k@10 < k@5 in sort order")
	}
}

func TestOrderingByUserKeyFirst(t *testing.T) {
	// Different user keys order by user key regardless of timestamp.
	a := Encode([]byte("a"), 1)  // small ts
	b := Encode([]byte("b"), 999) // large ts
	if Compare(a, b) >= 0 {
		t.Fatalf("expected a@1 < b@999")
	}
}

func TestCompareUserKey(t *testing.T) {
	a := Encode([]byte("a"), 7)
	b := Encode([]byte("a"), 99)
	if CompareUserKey(a, b) != 0 {
		t.Fatalf("same user key should compare equal")
	}
	c := Encode([]byte("b"), 1)
	if CompareUserKey(a, c) >= 0 {
		t.Fatalf("a < b by user key")
	}
}

func TestRangeBeginIsNewest(t *testing.T) {
	// Encode(k, TsRangeBegin) must sort at or before any real version of k.
	begin := Encode([]byte("k"), TsRangeBegin)
	real := Encode([]byte("k"), 1000)
	if Compare(begin, real) > 0 {
		t.Fatalf("range-begin should be <= any real version")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/key/`
Expected: build failure — `undefined: Encode`.

- [ ] **Step 3: Implement the encoding**

Replace `internal/key/key.go` with:
```go
// Package key centralizes key ordering and timestamped (MVCC) key encoding.
//
// A stored key is userKey || bigEndian(^ts): an 8-byte timestamp suffix, bit-
// complemented so bytes.Compare orders equal user keys by timestamp descending
// (newest version first).
package key

import (
	"bytes"
	"encoding/binary"
)

// tsLen is the size of the encoded timestamp suffix in bytes.
const tsLen = 8

const (
	// TsRangeBegin encodes the newest possible version of a user key (used as a
	// lower scan bound to include all versions).
	TsRangeBegin = ^uint64(0)
	// TsRangeEnd encodes the oldest possible version.
	TsRangeEnd = uint64(0)
)

// Encode returns userKey || bigEndian(^ts).
func Encode(userKey []byte, ts uint64) []byte {
	out := make([]byte, len(userKey)+tsLen)
	copy(out, userKey)
	binary.BigEndian.PutUint64(out[len(userKey):], ^ts)
	return out
}

// UserKey returns the user-key portion of an encoded key.
func UserKey(encoded []byte) []byte {
	return encoded[:len(encoded)-tsLen]
}

// Timestamp returns the decoded timestamp of an encoded key.
func Timestamp(encoded []byte) uint64 {
	return ^binary.BigEndian.Uint64(encoded[len(encoded)-tsLen:])
}

// Compare orders encoded keys: user key ascending, then timestamp descending.
func Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// CompareUserKey compares only the user-key portions of two encoded keys.
func CompareUserKey(a, b []byte) int {
	return bytes.Compare(UserKey(a), UserKey(b))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/key/`
Expected: PASS.

- [ ] **Step 5: Confirm the rest of the codebase still builds**

Run: `go build ./... && go vet ./...`
Expected: builds (existing packages call only `key.Compare`, which is unchanged in signature).

- [ ] **Step 6: Commit**

```bash
git add internal/key/
git commit -m "feat: timestamped key encoding (userKey || ^ts)"
```

---

## Task 2: SSTable bloom-on-user-key and maxTs footer

**Files:**
- Modify: `internal/sstable/builder.go`
- Modify: `internal/sstable/sstable.go`
- Test: `internal/sstable/maxts_test.go`

New footer: `[data][meta][metaCRC u32][metaOffset u32][bloom][maxTs u64][bloomOffset u32]`.
The `bloomOffset` u32 stays the last 4 bytes (so `metaSectionStart`/`ReadBlock` are
unchanged); `maxTs` sits in the 8 bytes just before it.

- [ ] **Step 1: Write the failing test**

Create `internal/sstable/maxts_test.go`:
```go
package sstable

import (
	"fmt"
	"path/filepath"
	"testing"

	"mythdb/internal/bloom"
	"mythdb/internal/key"
)

func TestMaxTsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "1.sst")
	b := NewBuilder(256)
	// Encoded keys with increasing timestamps.
	for i := 0; i < 30; i++ {
		b.Add(key.Encode([]byte(fmt.Sprintf("k%03d", i)), uint64(i+1)), []byte("v"))
	}
	sst, err := b.Build(1, path)
	if err != nil {
		t.Fatal(err)
	}
	if sst.MaxTs() != 30 {
		t.Fatalf("built MaxTs = %d want 30", sst.MaxTs())
	}
	sst.Close()

	reopened, err := Open(1, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.MaxTs() != 30 {
		t.Fatalf("reopened MaxTs = %d want 30", reopened.MaxTs())
	}
}

func TestBloomIsOnUserKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "1.sst")
	b := NewBuilder(256)
	b.Add(key.Encode([]byte("apple"), 5), []byte("v"))
	sst, err := b.Build(1, path)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()
	// MayContain takes a USER-key hash; "apple" must be reported present.
	if !sst.bloom.MayContain(bloom.Hash([]byte("apple"))) {
		t.Fatal("bloom should contain user key 'apple'")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sstable/ -run 'TestMaxTs|TestBloomIsOnUserKey'`
Expected: build failure — `sst.MaxTs undefined` (and the bloom test fails because the bloom is currently built on the full encoded key).

- [ ] **Step 3: Update the builder (bloom on user key + record maxTs + footer)**

In `internal/sstable/builder.go`, add `"mythdb/internal/key"` to the import block. Add a `maxTs` field to the `Builder` struct:
```go
type Builder struct {
	blockBuilder *block.Builder
	blockSize    int
	data         []byte
	meta         []BlockMeta
	firstKey     []byte
	lastKey      []byte
	keyHashes    []uint32
	maxTs        uint64
}
```

In `Add`, hash the user key and track the max timestamp. Replace the first line of `Add`:
```go
	b.keyHashes = append(b.keyHashes, bloom.Hash(k))
```
with:
```go
	b.keyHashes = append(b.keyHashes, bloom.Hash(key.UserKey(k)))
	if ts := key.Timestamp(k); ts > b.maxTs {
		b.maxTs = ts
	}
```

In `Build`, write `maxTs` (8 bytes, big-endian) just before the bloom offset. Replace:
```go
	bl := bloom.Build(b.keyHashes, bloom.BitsPerKey(len(b.keyHashes), 0.01))
	bloomOffset := len(buf)
	buf = append(buf, bl.Encode()...)
	binary.LittleEndian.PutUint32(off4, uint32(bloomOffset))
	buf = append(buf, off4...)
```
with:
```go
	bl := bloom.Build(b.keyHashes, bloom.BitsPerKey(len(b.keyHashes), 0.01))
	bloomOffset := len(buf)
	buf = append(buf, bl.Encode()...)
	maxTsBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTsBuf, b.maxTs)
	buf = append(buf, maxTsBuf...)
	binary.LittleEndian.PutUint32(off4, uint32(bloomOffset))
	buf = append(buf, off4...)
```

In `Build`'s returned struct literal, add the `maxTs` field:
```go
	return &SsTable{
		file:      f,
		path:      path,
		id:        id,
		size:      int64(len(buf)),
		maxTs:     b.maxTs,
		blockMeta: b.meta,
		bloom:     bl,
		firstKey:  b.firstKey,
		lastKey:   append([]byte(nil), b.lastKey...),
	}, nil
```

- [ ] **Step 4: Update the reader (read maxTs + adjust bloom slice)**

In `internal/sstable/sstable.go`, add a `maxTs uint64` field to `SsTable` (after `size int64`):
```go
type SsTable struct {
	file      *os.File
	path      string
	id        int
	size      int64
	maxTs     uint64
	blockMeta []BlockMeta
	bloom     *bloom.Bloom
	firstKey  []byte
	lastKey   []byte
}
```

Raise the size guard from `if size < 12 {` to `if size < 20 {`.

Replace the bloom read block:
```go
	bloomOffBuf, err := read(size-4, 4)
	if err != nil {
		f.Close()
		return nil, err
	}
	bloomOff := int64(binary.LittleEndian.Uint32(bloomOffBuf))
	bloomBuf, err := read(bloomOff, size-4-bloomOff)
	if err != nil {
		f.Close()
		return nil, err
	}
	bl, err := bloom.Decode(bloomBuf)
	if err != nil {
		f.Close()
		return nil, err
	}
```
with (bloom now ends 8 bytes earlier; maxTs sits in those 8 bytes):
```go
	bloomOffBuf, err := read(size-4, 4)
	if err != nil {
		f.Close()
		return nil, err
	}
	bloomOff := int64(binary.LittleEndian.Uint32(bloomOffBuf))
	maxTsBuf, err := read(size-12, 8)
	if err != nil {
		f.Close()
		return nil, err
	}
	maxTs := binary.BigEndian.Uint64(maxTsBuf)
	bloomBuf, err := read(bloomOff, (size-12)-bloomOff)
	if err != nil {
		f.Close()
		return nil, err
	}
	bl, err := bloom.Decode(bloomBuf)
	if err != nil {
		f.Close()
		return nil, err
	}
```

Set `maxTs` on the returned table. Replace:
```go
	t := &SsTable{file: f, path: path, id: id, size: size, blockMeta: metas, bloom: bl}
```
with:
```go
	t := &SsTable{file: f, path: path, id: id, size: size, maxTs: maxTs, blockMeta: metas, bloom: bl}
```

Add the `MaxTs` accessor near `Size`:
```go
// MaxTs returns the maximum timestamp of any key in the table.
func (t *SsTable) MaxTs() uint64 { return t.maxTs }
```

Update `MayContain` to hash only the user key (callers pass a user key):
```go
// MayContain consults the bloom filter for a user key.
func (t *SsTable) MayContain(userKey []byte) bool { return t.bloom.MayContain(bloom.Hash(userKey)) }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/sstable/ && go vet ./internal/sstable/`
Expected: PASS. (Existing SST tests use raw, non-encoded keys; `key.Timestamp`/`key.UserKey` of a ≥8-byte raw key still parse without panic, and those tests only check ordering/lookups by the same bytes they stored, so they remain green. The metaSectionStart/ReadBlock paths are unchanged.)

- [ ] **Step 6: Run the whole suite**

Run: `go test ./...`
Expected: PASS. (The `lsm` engine still stores raw keys at this point — Task 3 switches it to encoded keys. The SST format change is symmetric, so reads still work.)

> If any existing `sstable` test fails because a stored key is shorter than 8 bytes
> (so `key.UserKey`/`key.Timestamp` would slice out of range), that is acceptable to
> see ONLY at this step if such a test exists; the current sstable tests use keys
> like `key00001` (≥8 bytes) and `a`/`b`/`c`. Keys shorter than 8 bytes are not used
> in sstable tests. If a panic occurs, report it — do not paper over it.

- [ ] **Step 7: Commit**

```bash
git add internal/sstable/
git commit -m "feat: SST bloom on user key and maxTs in footer"
```

---

## Task 3: Engine MVCC core (commit ts, encoded writes, mvccIterator, MVCC Get/Scan)

**Files:**
- Create: `internal/lsm/mvcc.go`
- Create: `internal/lsm/mvcc_iterator.go`
- Modify: `internal/lsm/iterator.go` (remove `lsmIterator`; keep `fusedIterator`)
- Modify: `internal/lsm/storage.go` (mvcc field; Open init; encoded `Write`; MVCC `Get`/`Scan`)
- Test: `internal/lsm/mvcc_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/lsm/mvcc_test.go`:
```go
package lsm

import (
	"fmt"
	"testing"
)

func TestMVCCOverwriteReadsNewest(t *testing.T) {
	s, err := Open(Options{Path: t.TempDir(), BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Put([]byte("k"), []byte("v1"))
	s.Put([]byte("k"), []byte("v2"))
	s.Put([]byte("k"), []byte("v3"))
	v, ok, _ := s.Get([]byte("k"))
	if !ok || string(v) != "v3" {
		t.Fatalf("get k = %q ok=%v want v3", v, ok)
	}
}

func TestMVCCScanDedupsUserKeys(t *testing.T) {
	s, err := Open(Options{Path: t.TempDir(), BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Three versions of "b", one each of "a" and "c".
	s.Put([]byte("a"), []byte("a1"))
	s.Put([]byte("b"), []byte("b1"))
	s.Put([]byte("b"), []byte("b2"))
	s.Put([]byte("b"), []byte("b3"))
	s.Put([]byte("c"), []byte("c1"))

	it, err := s.Scan(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	type kv struct{ k, v string }
	var got []kv
	for it.IsValid() {
		got = append(got, kv{string(it.Key()), string(it.Value())})
		it.Next()
	}
	want := []kv{{"a", "a1"}, {"b", "b3"}, {"c", "c1"}}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at %d got %v want %v", i, got[i], want[i])
		}
	}
}

func TestMVCCDeleteHidesAllVersions(t *testing.T) {
	s, err := Open(Options{Path: t.TempDir(), BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Put([]byte("k"), []byte("v1"))
	s.Put([]byte("k"), []byte("v2"))
	s.Delete([]byte("k"))
	if _, ok, _ := s.Get([]byte("k")); ok {
		t.Fatal("k should read as deleted (newest version is a tombstone)")
	}
	it, _ := s.Scan(nil, nil)
	if it.IsValid() {
		t.Fatalf("scan should be empty, first=%q", it.Key())
	}
}

func TestCommitTimestampsAreMonotonic(t *testing.T) {
	s, err := Open(Options{Path: t.TempDir(), BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 100; i++ {
		s.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
	}
	if s.mvcc.latestTs() != 100 {
		t.Fatalf("latestTs = %d want 100", s.mvcc.latestTs())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/lsm/ -run 'TestMVCC|TestCommitTimestamps'`
Expected: build failure — `s.mvcc undefined`.

- [ ] **Step 3: Create the commit-timestamp source**

Create `internal/lsm/mvcc.go`:
```go
package lsm

import "sync"

// mvcc holds the monotonic commit-timestamp counter. Week 3B extends this into a
// full transaction controller (watermark, committed history); for 3A it is just
// the clock.
type mvcc struct {
	mu sync.Mutex
	ts uint64
}

func newMvcc(initial uint64) *mvcc { return &mvcc{ts: initial} }

// latestTs returns the latest committed timestamp.
func (m *mvcc) latestTs() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ts
}

// nextTs increments and returns the new commit timestamp.
func (m *mvcc) nextTs() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ts++
	return m.ts
}

// setTs raises the counter to at least ts (used during recovery).
func (m *mvcc) setTs(ts uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ts > m.ts {
		m.ts = ts
	}
}
```

- [ ] **Step 4: Create the MVCC iterator**

Create `internal/lsm/mvcc_iterator.go`:
```go
package lsm

import (
	"bytes"

	"mythdb/internal/iterator"
	"mythdb/internal/key"
)

// mvccIterator reads a merged stream of encoded keys (user key ascending, ts
// descending) at a fixed read timestamp. For each user key it yields the newest
// version with ts <= readTs, skipping tombstones, and decodes the user key.
type mvccIterator struct {
	inner  iterator.StorageIterator // encoded keys
	readTs uint64
	upper  []byte // exclusive user-key bound; nil = unbounded
	curKey []byte // current decoded user key (nil when exhausted)
	curVal []byte
	err    error
}

func newMvccIterator(inner iterator.StorageIterator, readTs uint64, upper []byte) (*mvccIterator, error) {
	it := &mvccIterator{inner: inner, readTs: readTs, upper: upper}
	if err := it.findNext(); err != nil {
		return nil, err
	}
	return it, nil
}

// findNext positions on the next visible (user key, value), or marks exhausted.
func (it *mvccIterator) findNext() error {
	for it.inner.IsValid() {
		user := key.UserKey(it.inner.Key())
		if it.upper != nil && bytes.Compare(user, it.upper) >= 0 {
			it.curKey = nil
			return nil
		}
		// Skip versions of this user key newer than readTs.
		for it.inner.IsValid() &&
			bytes.Equal(key.UserKey(it.inner.Key()), user) &&
			key.Timestamp(it.inner.Key()) > it.readTs {
			if err := it.inner.Next(); err != nil {
				it.err = err
				return err
			}
		}
		if !it.inner.IsValid() || !bytes.Equal(key.UserKey(it.inner.Key()), user) {
			continue // no version <= readTs for this user key; move on
		}
		val := it.inner.Value()
		if len(val) == 0 {
			// Tombstone: skip all remaining versions of this user key.
			if err := it.skipUser(user); err != nil {
				return err
			}
			continue
		}
		it.curKey = append([]byte(nil), user...)
		it.curVal = append([]byte(nil), val...)
		return nil
	}
	it.curKey = nil
	return nil
}

// skipUser advances past every remaining version of the given user key.
func (it *mvccIterator) skipUser(user []byte) error {
	for it.inner.IsValid() && bytes.Equal(key.UserKey(it.inner.Key()), user) {
		if err := it.inner.Next(); err != nil {
			it.err = err
			return err
		}
	}
	return nil
}

func (it *mvccIterator) IsValid() bool { return it.err == nil && it.curKey != nil }
func (it *mvccIterator) Key() []byte   { return it.curKey }
func (it *mvccIterator) Value() []byte { return it.curVal }

func (it *mvccIterator) Next() error {
	if it.curKey == nil {
		return nil
	}
	prev := it.curKey
	it.curKey = nil
	if err := it.skipUser(prev); err != nil {
		return err
	}
	return it.findNext()
}
```

- [ ] **Step 5: Remove the obsolete lsmIterator**

In `internal/lsm/iterator.go`, delete the `lsmIterator` type and its methods
(`newLsmIterator`, `inBound`, `skipDeleted`, `IsValid`/`Key`/`Value`/`Next` for
`lsmIterator`) — everything from the `// lsmIterator wraps...` comment down to just
before `// fusedIterator guards...`. Keep `fusedIterator` and its methods. The file
should keep only the `fusedIterator` type (and its imports `iterator`; `key` is no
longer needed there — remove the `key` import if it becomes unused).

The resulting `internal/lsm/iterator.go` is:
```go
package lsm

import "mythdb/internal/iterator"

// fusedIterator guards against use after exhaustion or error: once invalid it
// stays invalid and never calls the wrapped iterator again.
type fusedIterator struct {
	inner  iterator.StorageIterator
	hasErr bool
}

func newFusedIterator(inner iterator.StorageIterator) *fusedIterator {
	return &fusedIterator{inner: inner}
}

func (it *fusedIterator) IsValid() bool { return !it.hasErr && it.inner.IsValid() }
func (it *fusedIterator) Key() []byte   { return it.inner.Key() }
func (it *fusedIterator) Value() []byte { return it.inner.Value() }

func (it *fusedIterator) Next() error {
	if it.hasErr {
		return nil
	}
	if !it.inner.IsValid() {
		return nil
	}
	if err := it.inner.Next(); err != nil {
		it.hasErr = true
		return err
	}
	return nil
}
```
- [ ] **Step 6: Add the mvcc field, initialize it in Open, and restore it on recovery**

In `internal/lsm/storage.go`, add a `mvcc` field to `Storage`:
```go
type Storage struct {
	mu   sync.RWMutex
	st   *state
	opts Options

	idMu   sync.Mutex
	nextID int

	controller compaction.Controller
	manifest   *manifest.Manifest
	mvcc       *mvcc

	stopCh chan struct{}
	wg     sync.WaitGroup
}
```

In `Open`, in the fresh-start branch (the `else` block), after `s.manifest = man`
and before creating the memtable, add:
```go
		s.mvcc = newMvcc(0)
```

In `internal/lsm/recover.go`, add `"mythdb/internal/key"` to the import block. Just
before the orphan-cleanup section (`keepWAL := ...`) and after `s.st = &state{...}`
is assigned, compute the maximum timestamp on disk and install the clock so new
writes outrank every recovered version:
```go
	// Restore the commit-timestamp counter to the maximum timestamp on disk.
	var maxTs uint64
	for _, sst := range sstables {
		if t := sst.MaxTs(); t > maxTs {
			maxTs = t
		}
	}
	for _, mt := range imm {
		mtIt := mt.Iter(nil, nil)
		for mtIt.IsValid() {
			if t := key.Timestamp(mtIt.Key()); t > maxTs {
				maxTs = t
			}
			mtIt.Next()
		}
	}
	s.mvcc = newMvcc(maxTs)
```

- [ ] **Step 7: Encode keys in the write path**

In `internal/lsm/storage.go` (`key` is already imported), replace `Write` so each
op's key is encoded with the batch's commit timestamp:
```go
// Write applies all operations of b under one lock at a single commit timestamp:
// each op is encoded with that timestamp, logged to the WAL, and inserted.
func (s *Storage) Write(b *WriteBatch) error {
	s.mu.Lock()
	commitTs := s.mvcc.nextTs()
	entries := make([]memtable.Entry, len(b.ops))
	for i, op := range b.ops {
		entries[i] = memtable.Entry{Key: key.Encode(op.key, commitTs), Value: op.value}
	}
	if err := s.st.memtable.PutBatch(entries); err != nil {
		s.mu.Unlock()
		return err
	}
	full := s.st.memtable.ApproximateSize() >= s.opts.TargetSSTSize
	s.mu.Unlock()
	if full {
		return s.ForceFreezeMemtable()
	}
	return nil
}
```

- [ ] **Step 8: Replace Get/Scan with the MVCC read path**

In `internal/lsm/storage.go`, add `"bytes"` to the import block. Replace the entire
`Get` and `Scan` functions and delete the now-unused `getFromSST` and `resolve`
helpers. Add the shared `buildMvccScan` helper. Use exactly:
```go
// buildMvccScan builds a fused MVCC iterator over the user-key range [lower, upper)
// at readTs. lower/upper are user keys; nil means unbounded.
func (s *Storage) buildMvccScan(lower, upper []byte, readTs uint64) (iterator.StorageIterator, error) {
	st := s.snapshot()

	var encLower []byte
	if lower != nil {
		encLower = key.Encode(lower, key.TsRangeBegin)
	}
	var encUpper []byte
	if upper != nil {
		encUpper = key.Encode(upper, key.TsRangeBegin)
	}

	memIters := []iterator.StorageIterator{st.memtable.Iter(encLower, encUpper)}
	for _, m := range st.immMemtables {
		memIters = append(memIters, m.Iter(encLower, encUpper))
	}
	memMerge := iterator.NewMergeIterator(memIters)

	var l0Iters []iterator.StorageIterator
	for _, id := range st.l0 {
		it, err := seekSST(st.sstables[id], encLower)
		if err != nil {
			return nil, err
		}
		l0Iters = append(l0Iters, it)
	}
	l0Merge := iterator.NewMergeIterator(l0Iters)

	allIters := []iterator.StorageIterator{memMerge, l0Merge}
	for _, level := range st.levels {
		tables := make([]*sstable.SsTable, len(level))
		for i, id := range level {
			tables[i] = st.sstables[id]
		}
		var ci iterator.StorageIterator
		var err error
		if encLower == nil {
			ci, err = iterator.NewConcatIterAndSeekToFirst(tables)
		} else {
			ci, err = iterator.NewConcatIterAndSeekToKey(tables, encLower)
		}
		if err != nil {
			return nil, err
		}
		allIters = append(allIters, ci)
	}
	merged := iterator.NewMergeIterator(allIters)

	mv, err := newMvccIterator(merged, readTs, upper)
	if err != nil {
		return nil, err
	}
	return newFusedIterator(mv), nil
}

// Get returns the value for k at the latest committed timestamp.
func (s *Storage) Get(k []byte) ([]byte, bool, error) {
	readTs := s.mvcc.latestTs()
	it, err := s.buildMvccScan(k, nil, readTs)
	if err != nil {
		return nil, false, err
	}
	if it.IsValid() && bytes.Equal(it.Key(), k) {
		return append([]byte(nil), it.Value()...), true, nil
	}
	return nil, false, nil
}

// Scan returns an iterator over [lower, upper) at the latest committed timestamp.
func (s *Storage) Scan(lower, upper []byte) (iterator.StorageIterator, error) {
	return s.buildMvccScan(lower, upper, s.mvcc.latestTs())
}
```

> The `resolve` helper is no longer referenced after this change — delete it. The
> `seekSST` helper IS still used by `buildMvccScan` — keep it. After deleting
> `getFromSST`, confirm `key` is still imported (it is, used in `Write` and
> `buildMvccScan`).

- [ ] **Step 9: Keep all versions during compaction**

In `internal/lsm/compact.go`, `doCompact` currently drops tombstones at the bottom
level. Remove that drop so every version survives (Week 3B reintroduces GC using the
watermark). Replace this block in the merge loop:
```go
	for merged.IsValid() {
		k := merged.Key()
		v := merged.Value()
		if toBottomLevel && len(v) == 0 {
			if err := merged.Next(); err != nil {
				return nil, err
			}
			continue
		}
		if builder == nil {
			builder = sstable.NewBuilder(s.opts.BlockSize)
		}
		builder.Add(k, v)
		if int64(builder.EstimatedSize()) >= s.opts.TargetSSTSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		if err := merged.Next(); err != nil {
			return nil, err
		}
	}
```
with (no tombstone dropping in 3A — `toBottomLevel` is retained for Week 3B):
```go
	_ = toBottomLevel // Week 3B uses this with the watermark to GC old versions.
	for merged.IsValid() {
		k := merged.Key()
		v := merged.Value()
		if builder == nil {
			builder = sstable.NewBuilder(s.opts.BlockSize)
		}
		builder.Add(k, v)
		if int64(builder.EstimatedSize()) >= s.opts.TargetSSTSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		if err := merged.Next(); err != nil {
			return nil, err
		}
	}
```

- [ ] **Step 10: Add recovery + compaction regression tests**

Create `internal/lsm/mvcc_recover_test.go` (these lock in the recovery commit-ts
restore from Step 6 and the version-retaining compaction from Step 9):
```go
package lsm

import "testing"

func TestRecoverRestoresCommitTimestamp(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Path: dir, BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		s.Put([]byte("k"), []byte("v")) // 10 versions of k, ts 1..10
	}
	s.Close()

	s2, err := Open(Options{Path: dir, BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.mvcc.latestTs() < 10 {
		t.Fatalf("recovered latestTs = %d, want >= 10", s2.mvcc.latestTs())
	}
	s2.Put([]byte("k"), []byte("new"))
	v, ok, _ := s2.Get([]byte("k"))
	if !ok || string(v) != "new" {
		t.Fatalf("after recovery+write, k = %q ok=%v want new", v, ok)
	}
}

func TestRecoverCommitTsFromFlushedSST(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Path: dir, BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		s.Put([]byte("k"), []byte("v"))
	}
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable() // data now in an SST, WAL gone
	s.Close()

	s2, err := Open(Options{Path: dir, BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.mvcc.latestTs() < 10 {
		t.Fatalf("recovered latestTs from SST = %d, want >= 10", s2.mvcc.latestTs())
	}
}

func TestCompactionRetainsVersionsForReads(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{
		Path: dir, BlockSize: 256, TargetSSTSize: 1 << 20,
		Compaction: CompactionOptions{Strategy: "full"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Put([]byte("k"), []byte("old"))
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()
	s.Put([]byte("k"), []byte("new"))
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()
	if _, err := s.runOnceCompaction(); err != nil {
		t.Fatal(err)
	}
	v, ok, _ := s.Get([]byte("k"))
	if !ok || string(v) != "new" {
		t.Fatalf("after compaction k = %q ok=%v want new", v, ok)
	}
}
```

- [ ] **Step 11: Run the whole suite, the race detector, and the demo**

Run: `go test ./... && go vet ./... && go test -race ./internal/lsm/`
Expected: PASS everywhere; no data races. In particular every Week 1/2 lsm test
stays green (latest-read semantics); `TestFullCompactionDropsTombstones` (Week 2A)
passes because the MVCC scan skips user keys whose newest version is a tombstone,
even though versions are now retained.

Run: `go run ./cmd/mythdb`
Expected: same observable output as before (key03 deleted, key05 UPDATED, bounded
scan over key02..key07) — MVCC must not change single-version observable results.

- [ ] **Step 12: Commit**

```bash
git add internal/lsm/
git commit -m "feat: MVCC engine core — commit timestamps, encoded writes, mvcc read iterator, version-retaining compaction, ts recovery"
```

---

## Self-Review Notes

- **Spec coverage:** key encoding (Task 1), bloom-on-user-key + maxTs (Task 2),
  commit-ts source + encoded writes + mvccIterator + MVCC Get/Scan + recovery
  commit-ts restore + version-retaining compaction (Task 3). All 3A spec components
  map to a task.
- **Task ordering:** Task 3 lands the storage, recovery, and compaction changes
  together because they are interdependent — recovery must restore the commit ts and
  compaction must retain versions, or the existing recovery / full-compaction tests
  would fail. The whole suite is green only at the end of Task 3, which is why those
  changes are not split into a later task.
- **Type consistency:** `key.{Encode,UserKey,Timestamp,Compare,CompareUserKey,TsRangeBegin,TsRangeEnd}`;
  `sstable.SsTable.MaxTs()` + `MayContain(userKey)`; `mvcc.{newMvcc,latestTs,nextTs,setTs}`;
  `mvccIterator` + `newMvccIterator`; engine `buildMvccScan`. `lsmIterator` removed;
  `fusedIterator` kept and now wraps `mvccIterator`.
- **Backward compatibility:** existing Week 1/2 tests read at the latest ts, which
  the MVCC path reproduces. Memtable stays byte-agnostic. The SST footer change is
  symmetric and keeps `metaSectionStart`/`ReadBlock` untouched (bloomOffset is still
  the last 4 bytes).
- **Recovery correctness:** commit ts restored from `max(SST.MaxTs, max key ts in
  recovered memtables)`, so post-recovery writes strictly outrank recovered versions.
- **Deferred to 3B:** transactions, watermark, version GC (the `toBottomLevel`
  parameter is retained but inert in 3A).
