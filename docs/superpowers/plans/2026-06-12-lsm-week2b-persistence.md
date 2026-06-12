# LSM Tree Week 2B (Persistence) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the LSM engine durable: a manifest + per-memtable WALs let `Open` rebuild full state after a crash, with a WriteBatch write path and CRC-checked records.

**Architecture:** A new `wal` package logs every write to the active memtable; a new `manifest` package records structural changes (new memtable / flush / compaction). Memtables optionally own a WAL. On `Open`, if a MANIFEST exists, the engine replays it to rebuild L0/levels and replays unflushed WALs into memtables.

**Tech Stack:** Go 1.26, standard library only (`encoding/binary`, `encoding/json`, `hash/crc32`, `os`, `sort`). Builds on Week 1 + 2A.

**Spec:** `docs/superpowers/specs/2026-06-12-lsm-week2b-persistence-design.md`.

**Conventions:**
- Module `mythdb`. Run tests from repo root. Commit after each task; use `git -c user.name='Claude' -c user.email='noreply@anthropic.com' commit` if identity is unset.
- After each task run `go test ./...` AND `go vet ./...`; confirm green before committing.
- WAL file = `<id>.wal`, manifest = `MANIFEST`, both under `Options.Path`.
- A flushed SST reuses its memtable's id (SST `<id>.sst` ↔ memtable `<id>`).

---

## File Structure

```
internal/
  wal/wal.go              (new)    framed (key,value)+crc records
  manifest/manifest.go    (new)    framed JSON records+crc
  sstable/builder.go      (modify) write meta-section CRC
  sstable/sstable.go      (modify) verify meta-section CRC on Open
  memtable/memtable.go    (modify) optional WAL ownership; Put returns error
  lsm/storage.go          (modify) WriteBatch+Write; persistent Open; manifest on freeze/flush; Close
  lsm/compact.go          (modify) manifest Compaction record after swap
  lsm/recover.go          (new)    rebuild state from manifest + WALs
```

---

## Task 1: WAL package

**Files:**
- Create: `internal/wal/wal.go`
- Test: `internal/wal/wal_test.go`

Record format (little-endian): `keyLen(u32) key valLen(u32) value crc32(u32)`, CRC over `keyLen..value`.

- [ ] **Step 1: Write the failing test**

Create `internal/wal/wal_test.go`:
```go
package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendAndRecover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0.wal")
	w, err := Create(path, false)
	if err != nil {
		t.Fatal(err)
	}
	w.Put([]byte("a"), []byte("1"))
	w.Put([]byte("b"), []byte{}) // tombstone
	w.Put([]byte("c"), []byte("3"))
	w.Close()

	recs, w2, err := Recover(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if len(recs) != 3 {
		t.Fatalf("got %d records, want 3", len(recs))
	}
	if string(recs[0].Key) != "a" || string(recs[0].Value) != "1" {
		t.Fatalf("rec0 = %q/%q", recs[0].Key, recs[0].Value)
	}
	if string(recs[1].Key) != "b" || len(recs[1].Value) != 0 {
		t.Fatalf("rec1 tombstone = %q/%q", recs[1].Key, recs[1].Value)
	}
}

func TestRecoverMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.wal")
	recs, w, err := Recover(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if len(recs) != 0 {
		t.Fatalf("missing file should recover 0 records, got %d", len(recs))
	}
}

func TestRecoverDropsTruncatedTrailingRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0.wal")
	w, _ := Create(path, false)
	w.Put([]byte("a"), []byte("1"))
	w.Close()
	// Append a partial (truncated) record: a keyLen header with no body.
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	hdr := make([]byte, 4)
	binary.LittleEndian.PutUint32(hdr, 99) // claims a 99-byte key that isn't there
	f.Write(hdr)
	f.Close()

	recs, w2, err := Recover(path, false)
	if err != nil {
		t.Fatalf("truncated trailing record should be dropped, got err %v", err)
	}
	defer w2.Close()
	if len(recs) != 1 || string(recs[0].Key) != "a" {
		t.Fatalf("expected 1 intact record, got %d", len(recs))
	}
}

func TestRecoverDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0.wal")
	w, _ := Create(path, false)
	w.Put([]byte("aaaa"), []byte("bbbb"))
	w.Close()
	// Flip a byte inside the (complete) record's value.
	data, _ := os.ReadFile(path)
	data[10] ^= 0xff
	os.WriteFile(path, data, 0o644)

	if _, _, err := Recover(path, false); err == nil {
		t.Fatal("expected checksum error for corrupted complete record")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/wal/`
Expected: build failure — `undefined: Create`.

- [ ] **Step 3: Write the implementation**

Create `internal/wal/wal.go`:
```go
// Package wal implements a write-ahead log: a sequence of CRC-checked
// (key, value) records appended before a memtable mutation.
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

// WAL appends records to a file.
type WAL struct {
	f          *os.File
	syncWrites bool
}

// Record is one recovered key/value pair. An empty Value is a tombstone.
type Record struct {
	Key, Value []byte
}

// Create starts a fresh (truncated) WAL at path.
func Create(path string, syncWrites bool) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f, syncWrites: syncWrites}, nil
}

// Recover reads all intact records from path and returns a WAL positioned to
// append. A truncated/incomplete trailing record is dropped (the file is
// truncated to the last intact record). A CRC mismatch on a complete record
// returns an error. A missing file recovers zero records.
func Recover(path string, syncWrites bool) ([]Record, *WAL, error) {
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}
	recs, consumed, perr := parseRecords(data)
	if perr != nil {
		return nil, nil, perr
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, err
	}
	// Drop any partial trailing bytes so future appends stay well-formed.
	if err := f.Truncate(consumed); err != nil {
		f.Close()
		return nil, nil, err
	}
	if _, err := f.Seek(consumed, 0); err != nil {
		f.Close()
		return nil, nil, err
	}
	return recs, &WAL{f: f, syncWrites: syncWrites}, nil
}

// parseRecords returns intact records and the byte offset up to which the file
// is well-formed. An incomplete trailing record stops parsing (no error); a
// complete record with a bad CRC returns an error.
func parseRecords(data []byte) ([]Record, int64, error) {
	var recs []Record
	pos := 0
	for {
		if pos+4 > len(data) {
			break
		}
		keyLen := int(binary.LittleEndian.Uint32(data[pos:]))
		keyStart := pos + 4
		if keyStart+keyLen+4 > len(data) {
			break
		}
		valLen := int(binary.LittleEndian.Uint32(data[keyStart+keyLen:]))
		valStart := keyStart + keyLen + 4
		if valStart+valLen+4 > len(data) {
			break
		}
		crcPos := valStart + valLen
		want := binary.LittleEndian.Uint32(data[crcPos:])
		if crc32.ChecksumIEEE(data[pos:crcPos]) != want {
			return recs, int64(pos), fmt.Errorf("wal: checksum mismatch at offset %d", pos)
		}
		k := append([]byte(nil), data[keyStart:keyStart+keyLen]...)
		v := append([]byte(nil), data[valStart:valStart+valLen]...)
		recs = append(recs, Record{Key: k, Value: v})
		pos = crcPos + 4
	}
	return recs, int64(pos), nil
}

// Put appends one record, writing through to the file. fsync if syncWrites.
func (w *WAL) Put(key, value []byte) error {
	buf := make([]byte, 0, 4+len(key)+4+len(value)+4)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(key)))
	buf = append(buf, key...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(value)))
	buf = append(buf, value...)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))
	if _, err := w.f.Write(buf); err != nil {
		return err
	}
	if w.syncWrites {
		return w.f.Sync()
	}
	return nil
}

// Sync flushes buffered data to stable storage.
func (w *WAL) Sync() error { return w.f.Sync() }

// Close closes the underlying file.
func (w *WAL) Close() error { return w.f.Close() }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/wal/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/wal/
git commit -m "feat: write-ahead log with crc records and crash-safe recovery"
```

---

## Task 2: Manifest package

**Files:**
- Create: `internal/manifest/manifest.go`
- Test: `internal/manifest/manifest_test.go`

Record framing: `len(u32) jsonBytes crc32(u32)`, CRC over `jsonBytes`.

- [ ] **Step 1: Write the failing test**

Create `internal/manifest/manifest_test.go`:
```go
package manifest

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestAddAndRecover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")
	m, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []Record{
		{Kind: KindNewMemtable, ID: 0},
		{Kind: KindFlush, ID: 0},
		{Kind: KindNewMemtable, ID: 1},
		{Kind: KindCompaction, UpperLevel: 0, UpperIDs: []int{0}, LowerLevel: 1, LowerIDs: nil, NewIDs: []int{5}},
	}
	for _, r := range want {
		if err := m.AddRecord(r); err != nil {
			t.Fatal(err)
		}
	}
	m.Close()

	got, m2, err := Recover(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	if len(got) != len(want) {
		t.Fatalf("got %d records want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Kind != want[i].Kind || got[i].ID != want[i].ID {
			t.Fatalf("record %d = %+v want %+v", i, got[i], want[i])
		}
	}
	if !reflect.DeepEqual(got[3].NewIDs, []int{5}) || !reflect.DeepEqual(got[3].UpperIDs, []int{0}) {
		t.Fatalf("compaction record = %+v", got[3])
	}
}

func TestRecoverDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")
	m, _ := Create(path)
	m.AddRecord(Record{Kind: KindNewMemtable, ID: 7})
	m.Close()
	data, _ := os.ReadFile(path)
	data[6] ^= 0xff // flip a byte inside the json payload
	os.WriteFile(path, data, 0o644)
	if _, _, err := Recover(path); err == nil {
		t.Fatal("expected checksum error")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/manifest/`
Expected: build failure — `undefined: Create`.

- [ ] **Step 3: Write the implementation**

Create `internal/manifest/manifest.go`:
```go
// Package manifest is an append-only log of structural LSM changes used to
// rebuild the engine's level layout on restart.
package manifest

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
)

// RecordKind identifies a manifest record.
type RecordKind string

const (
	KindNewMemtable RecordKind = "new_memtable"
	KindFlush       RecordKind = "flush"
	KindCompaction  RecordKind = "compaction"
)

// Record is one structural change. Fields not relevant to a kind stay zero.
type Record struct {
	Kind       RecordKind `json:"kind"`
	ID         int        `json:"id,omitempty"`
	UpperLevel int        `json:"upper_level,omitempty"`
	UpperIDs   []int      `json:"upper_ids,omitempty"`
	LowerLevel int        `json:"lower_level,omitempty"`
	LowerIDs   []int      `json:"lower_ids,omitempty"`
	NewIDs     []int      `json:"new_ids,omitempty"`
}

// Manifest appends records to a file.
type Manifest struct {
	f *os.File
}

// Create starts a fresh (truncated) manifest at path.
func Create(path string) (*Manifest, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &Manifest{f: f}, nil
}

// Recover reads all records and returns a manifest positioned to append.
func Recover(path string) ([]Record, *Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}
	var recs []Record
	pos := 0
	for {
		if pos+4 > len(data) {
			break
		}
		n := int(binary.LittleEndian.Uint32(data[pos:]))
		jsonStart := pos + 4
		if jsonStart+n+4 > len(data) {
			break // incomplete trailing record
		}
		payload := data[jsonStart : jsonStart+n]
		want := binary.LittleEndian.Uint32(data[jsonStart+n:])
		if crc32.ChecksumIEEE(payload) != want {
			return nil, nil, fmt.Errorf("manifest: checksum mismatch at offset %d", pos)
		}
		var r Record
		if err := json.Unmarshal(payload, &r); err != nil {
			return nil, nil, fmt.Errorf("manifest: bad record at offset %d: %w", pos, err)
		}
		recs = append(recs, r)
		pos = jsonStart + n + 4
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, err
	}
	if _, err := f.Seek(int64(pos), 0); err != nil {
		f.Close()
		return nil, nil, err
	}
	return recs, &Manifest{f: f}, nil
}

// AddRecord appends one record and fsyncs.
func (m *Manifest) AddRecord(r Record) error {
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	buf := binary.LittleEndian.AppendUint32(nil, uint32(len(payload)))
	buf = append(buf, payload...)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.ChecksumIEEE(payload))
	if _, err := m.f.Write(buf); err != nil {
		return err
	}
	return m.f.Sync()
}

// Close closes the underlying file.
func (m *Manifest) Close() error { return m.f.Close() }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/manifest/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/manifest/
git commit -m "feat: append-only manifest with crc-framed json records"
```

---

## Task 3: SST meta-section checksum

**Files:**
- Modify: `internal/sstable/builder.go`
- Modify: `internal/sstable/sstable.go`
- Test: `internal/sstable/metacrc_test.go`

New footer: `[data][meta][metaCRC u32][metaOffset u32][bloom][bloomOffset u32]`. The
`metaOffset` u32 stays the last 4 bytes before the bloom section, so
`metaSectionStart`/`ReadBlock` need no change.

- [ ] **Step 1: Write the failing test**

Create `internal/sstable/metacrc_test.go`:
```go
package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestMetaChecksumRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "1.sst")
	b := NewBuilder(256)
	for i := 0; i < 40; i++ {
		b.Add([]byte(fmt.Sprintf("key%03d", i)), []byte("v"))
	}
	if _, err := b.Build(1, path); err != nil {
		t.Fatal(err)
	}
	sst, err := Open(1, path)
	if err != nil {
		t.Fatalf("clean open failed: %v", err)
	}
	sst.Close()
}

func TestMetaChecksumDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "1.sst")
	b := NewBuilder(256)
	for i := 0; i < 40; i++ {
		b.Add([]byte(fmt.Sprintf("key%03d", i)), []byte("v"))
	}
	sst, _ := b.Build(1, path)
	sst.Close()

	data, _ := os.ReadFile(path)
	size := int64(len(data))
	bloomOff := int64(binary.LittleEndian.Uint32(data[size-4:]))
	metaOff := int64(binary.LittleEndian.Uint32(data[bloomOff-4:]))
	data[metaOff] ^= 0xff // corrupt the first meta byte
	os.WriteFile(path, data, 0o644)

	if _, err := Open(1, path); err == nil {
		t.Fatal("expected meta checksum error")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sstable/ -run TestMetaChecksum`
Expected: `TestMetaChecksumDetectsCorruption` FAILS (corruption currently undetected — Open succeeds).

- [ ] **Step 3: Write the meta CRC in builder.go**

In `internal/sstable/builder.go`, add `"hash/crc32"` to the import block. Replace this block in `Build`:
```go
	metaOffset := len(buf)
	buf = append(buf, encodeBlockMeta(b.meta)...)
	off4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(off4, uint32(metaOffset))
	buf = append(buf, off4...)
```
with:
```go
	metaOffset := len(buf)
	metaBytes := encodeBlockMeta(b.meta)
	buf = append(buf, metaBytes...)
	off4 := make([]byte, 4)
	// meta-section checksum, then the meta offset.
	binary.LittleEndian.PutUint32(off4, crc32.ChecksumIEEE(metaBytes))
	buf = append(buf, off4...)
	binary.LittleEndian.PutUint32(off4, uint32(metaOffset))
	buf = append(buf, off4...)
```

- [ ] **Step 4: Verify the meta CRC in sstable.go**

In `internal/sstable/sstable.go`, add `"hash/crc32"` to the import block. Change the file-too-small guard from `if size < 8 {` to `if size < 12 {`. Replace this block in `Open`:
```go
	metaOffBuf, err := read(bloomOff-4, 4)
	if err != nil {
		f.Close()
		return nil, err
	}
	metaOff := int64(binary.LittleEndian.Uint32(metaOffBuf))
	metaBuf, err := read(metaOff, (bloomOff-4)-metaOff)
	if err != nil {
		f.Close()
		return nil, err
	}
	metas, err := decodeBlockMeta(metaBuf)
```
with:
```go
	metaOffBuf, err := read(bloomOff-4, 4)
	if err != nil {
		f.Close()
		return nil, err
	}
	metaOff := int64(binary.LittleEndian.Uint32(metaOffBuf))
	metaCRCBuf, err := read(bloomOff-8, 4)
	if err != nil {
		f.Close()
		return nil, err
	}
	wantMetaCRC := binary.LittleEndian.Uint32(metaCRCBuf)
	metaBuf, err := read(metaOff, (bloomOff-8)-metaOff)
	if err != nil {
		f.Close()
		return nil, err
	}
	if crc32.ChecksumIEEE(metaBuf) != wantMetaCRC {
		f.Close()
		return nil, fmt.Errorf("sstable: meta checksum mismatch")
	}
	metas, err := decodeBlockMeta(metaBuf)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/sstable/ && go test ./... && go vet ./...`
Expected: PASS everywhere (existing SST tests still pass — the format change is symmetric; Week 1/2A SST reads go through `Open`/`Build`).

- [ ] **Step 6: Commit**

```bash
git add internal/sstable/
git commit -m "feat: checksum the SST meta section"
```

---

## Task 4: Memtable WAL ownership

**Files:**
- Modify: `internal/memtable/memtable.go`
- Test: `internal/memtable/wal_test.go`

`Put` now returns an `error` (WAL write may fail). Existing callers that ignore
the return still compile. Recovery replay inserts directly into the skiplist
(never re-logging).

- [ ] **Step 1: Write the failing test**

Create `internal/memtable/wal_test.go`:
```go
package memtable

import (
	"path/filepath"
	"testing"
)

func TestWALBackedPutRecovers(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "7.wal")
	m, err := NewWithWAL(7, walPath, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put([]byte("b"), []byte{}); err != nil { // tombstone
		t.Fatal(err)
	}
	if m.WALPath() != walPath {
		t.Fatalf("WALPath = %q", m.WALPath())
	}
	if err := m.CloseWAL(); err != nil {
		t.Fatal(err)
	}

	r, err := RecoverWAL(7, walPath, false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.CloseWAL()
	if r.ID() != 7 {
		t.Fatalf("id = %d", r.ID())
	}
	if v, ok := r.Get([]byte("a")); !ok || string(v) != "1" {
		t.Fatalf("recovered a = %q ok=%v", v, ok)
	}
	if v, ok := r.Get([]byte("b")); !ok || len(v) != 0 {
		t.Fatalf("recovered tombstone b = %q ok=%v", v, ok)
	}
}

func TestNewWithoutWALStillWorks(t *testing.T) {
	m := New(0)
	if err := m.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if m.WALPath() != "" {
		t.Fatalf("expected empty WALPath, got %q", m.WALPath())
	}
	if v, ok := m.Get([]byte("k")); !ok || string(v) != "v" {
		t.Fatalf("get = %q ok=%v", v, ok)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/memtable/ -run 'TestWAL|TestNewWithout'`
Expected: build failure — `undefined: NewWithWAL`, `m.Put used as value`.

- [ ] **Step 3: Modify memtable.go**

In `internal/memtable/memtable.go`, add the wal import:
```go
import (
	"sync"

	"mythdb/internal/key"
	"mythdb/internal/wal"
)
```

Add `wal` and `walPath` fields to the struct:
```go
type Memtable struct {
	mu         sync.RWMutex
	list       *skiplist
	id         int
	approxSize int64
	wal        *wal.WAL
	walPath    string
}
```

Add the constructors and WAL helpers (after `New`):
```go
// NewWithWAL creates a memtable backed by a fresh WAL at walPath.
func NewWithWAL(id int, walPath string, syncWrites bool) (*Memtable, error) {
	w, err := wal.Create(walPath, syncWrites)
	if err != nil {
		return nil, err
	}
	return &Memtable{list: newSkiplist(), id: id, wal: w, walPath: walPath}, nil
}

// RecoverWAL rebuilds a memtable by replaying an existing WAL, keeping it open
// for further appends.
func RecoverWAL(id int, walPath string, syncWrites bool) (*Memtable, error) {
	recs, w, err := wal.Recover(walPath, syncWrites)
	if err != nil {
		return nil, err
	}
	m := &Memtable{list: newSkiplist(), id: id, wal: w, walPath: walPath}
	for _, r := range recs {
		m.apply(r.Key, r.Value)
	}
	return m, nil
}

// apply inserts into the skiplist and updates the size estimate without logging.
func (m *Memtable) apply(k, v []byte) {
	inserted, oldLen := m.list.put(k, v)
	if inserted {
		m.approxSize += int64(len(k) + len(v))
	} else {
		m.approxSize += int64(len(v) - oldLen)
	}
}

// SyncWAL fsyncs the WAL if present.
func (m *Memtable) SyncWAL() error {
	if m.wal == nil {
		return nil
	}
	return m.wal.Sync()
}

// CloseWAL closes the WAL if present.
func (m *Memtable) CloseWAL() error {
	if m.wal == nil {
		return nil
	}
	return m.wal.Close()
}

// WALPath returns the WAL file path, or "" if the memtable has no WAL.
func (m *Memtable) WALPath() string { return m.walPath }
```

Replace `Put` so it logs to the WAL (if any) before inserting, and returns an error:
```go
// Put logs to the WAL (if present) then inserts or overwrites key with value.
func (m *Memtable) Put(k, v []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wal != nil {
		if err := m.wal.Put(k, v); err != nil {
			return err
		}
	}
	m.apply(k, v)
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/memtable/ && go vet ./internal/memtable/`
Expected: PASS (existing memtable tests still pass — they call `m.Put(...)` as a statement, ignoring the new error return).

- [ ] **Step 5: Run the whole suite to confirm the engine still compiles**

Run: `go test ./...`
Expected: PASS. The engine's existing `s.st.memtable.Put(k, v)` calls now ignore an error return (memtables there have no WAL, so Put never errors). Task 5 rewrites that path to handle the error.

- [ ] **Step 6: Commit**

```bash
git add internal/memtable/
git commit -m "feat: optional WAL ownership on memtable; Put returns error"
```

---

## Task 5: Engine WriteBatch, persistent Open, manifest on freeze/flush/compaction

**Files:**
- Modify: `internal/lsm/storage.go`
- Modify: `internal/lsm/compact.go`
- Test: `internal/lsm/persist_test.go`

This task makes the engine persist a manifest + WALs on a fresh `Open` (recovery
on reopen comes in Task 6). It adds `WriteBatch`/`Write`, routes `Put`/`Delete`
through it, makes flush reuse the memtable id as the SST id, and records
manifest entries on freeze/flush/compaction.

- [ ] **Step 1: Write the failing test**

Create `internal/lsm/persist_test.go`:
```go
package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteBatchAtomicVisible(t *testing.T) {
	s, err := Open(Options{Path: t.TempDir(), BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	b := &WriteBatch{}
	b.Put([]byte("a"), []byte("1"))
	b.Put([]byte("b"), []byte("2"))
	b.Delete([]byte("c"))
	if err := s.Write(b); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := s.Get([]byte("a")); !ok || string(v) != "1" {
		t.Fatalf("a = %q ok=%v", v, ok)
	}
	if v, ok, _ := s.Get([]byte("b")); !ok || string(v) != "2" {
		t.Fatalf("b = %q ok=%v", v, ok)
	}
	if _, ok, _ := s.Get([]byte("c")); ok {
		t.Fatal("c should be absent (deleted)")
	}
}

func TestFreshOpenWritesManifestAndWAL(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Path: dir, BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	s.Put([]byte("k"), []byte("v"))
	// MANIFEST and the active memtable's WAL (00000.wal) must exist on disk.
	if _, err := os.Stat(filepath.Join(dir, "MANIFEST")); err != nil {
		t.Fatalf("MANIFEST missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "00000.wal")); err != nil {
		t.Fatalf("WAL missing: %v", err)
	}
	s.Close()
}

func TestFlushReusesMemtableIDAndDeletesWAL(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Path: dir, BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 10; i++ {
		s.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v"))
	}
	// memtable 0 is active; freezing makes it immutable, flushing writes 00000.sst.
	if err := s.ForceFreezeMemtable(); err != nil {
		t.Fatal(err)
	}
	if err := s.ForceFlushNextImmMemtable(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "00000.sst")); err != nil {
		t.Fatalf("expected 00000.sst (memtable id reused): %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "00000.wal")); !os.IsNotExist(err) {
		t.Fatalf("flushed memtable WAL should be deleted, stat err=%v", err)
	}
	if v, ok, _ := s.Get([]byte("k05")); !ok || string(v) != "v" {
		t.Fatalf("k05 after flush = %q ok=%v", v, ok)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/lsm/ -run 'TestWriteBatch|TestFreshOpen|TestFlushReuses'`
Expected: build failure — `undefined: WriteBatch`, `s.Write undefined`.

- [ ] **Step 3: Update imports and Options/Storage in storage.go**

In `internal/lsm/storage.go`, update the import block to add `"os"` and `"mythdb/internal/manifest"`:
```go
import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"mythdb/internal/compaction"
	"mythdb/internal/iterator"
	"mythdb/internal/key"
	"mythdb/internal/manifest"
	"mythdb/internal/memtable"
	"mythdb/internal/sstable"
)
```

Add `SyncWrites` to `Options`:
```go
type Options struct {
	Path          string
	BlockSize     int
	TargetSSTSize int64

	Compaction CompactionOptions

	SyncWrites bool // fsync each WAL write (durable but slow); default off
}
```

Add a `manifest` field to `Storage`:
```go
type Storage struct {
	mu   sync.RWMutex
	st   *state
	opts Options

	idMu   sync.Mutex
	nextID int

	controller compaction.Controller
	manifest   *manifest.Manifest

	stopCh chan struct{}
	wg     sync.WaitGroup
}
```

Add a `walPath` helper next to `sstPath`:
```go
func (s *Storage) walPath(id int) string {
	return filepath.Join(s.opts.Path, fmt.Sprintf("%05d.wal", id))
}
```

- [ ] **Step 4: Replace Open with the persistent fresh-start version**

Replace the entire `Open` function in `storage.go` with:
```go
// Open initializes the engine. (Task 6 adds recovery from an existing MANIFEST;
// this version always starts fresh, creating the manifest and first WAL.)
func Open(opts Options) (*Storage, error) {
	if opts.BlockSize == 0 {
		opts.BlockSize = 4096
	}
	if opts.TargetSSTSize == 0 {
		opts.TargetSSTSize = 4 << 20
	}

	s := &Storage{opts: opts, nextID: 1}
	s.controller = buildController(opts.Compaction)

	if err := os.MkdirAll(opts.Path, 0o755); err != nil {
		return nil, err
	}
	man, err := manifest.Create(filepath.Join(opts.Path, "MANIFEST"))
	if err != nil {
		return nil, err
	}
	s.manifest = man

	mt, err := memtable.NewWithWAL(0, s.walPath(0), opts.SyncWrites)
	if err != nil {
		return nil, err
	}
	var levels [][]int
	if s.controller != nil {
		levels = make([][]int, s.controller.NumLevels())
	}
	s.st = &state{memtable: mt, levels: levels, sstables: map[int]*sstable.SsTable{}}
	if err := s.manifest.AddRecord(manifest.Record{Kind: manifest.KindNewMemtable, ID: 0}); err != nil {
		return nil, err
	}

	if s.controller != nil && opts.Compaction.Interval > 0 {
		s.startCompaction(opts.Compaction.Interval)
	}
	return s, nil
}
```

- [ ] **Step 5: Add WriteBatch + Write, and route Put/Delete through it**

In `storage.go`, replace the existing `Put` and `Delete` functions with the batch-based write path:
```go
// WriteBatch is an ordered group of writes applied atomically by Write.
type WriteBatch struct {
	ops []writeOp
}

type writeOp struct {
	key, value []byte
}

// Put stages an insert/overwrite.
func (b *WriteBatch) Put(key, value []byte) {
	b.ops = append(b.ops, writeOp{key: key, value: value})
}

// Delete stages a tombstone (empty value).
func (b *WriteBatch) Delete(key []byte) {
	b.ops = append(b.ops, writeOp{key: key, value: []byte{}})
}

// Write applies all operations of b under one lock: each is logged to the active
// memtable's WAL then inserted, so no reader observes a partial batch.
func (s *Storage) Write(b *WriteBatch) error {
	s.mu.Lock()
	for _, op := range b.ops {
		if err := s.st.memtable.Put(op.key, op.value); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	full := s.st.memtable.ApproximateSize() >= s.opts.TargetSSTSize
	s.mu.Unlock()
	if full {
		return s.ForceFreezeMemtable()
	}
	return nil
}

// Put inserts or overwrites a key.
func (s *Storage) Put(k, v []byte) error {
	b := &WriteBatch{}
	b.Put(k, v)
	return s.Write(b)
}

// Delete writes a tombstone for the key.
func (s *Storage) Delete(k []byte) error {
	b := &WriteBatch{}
	b.Delete(k)
	return s.Write(b)
}
```

- [ ] **Step 6: Update ForceFreezeMemtable to use a WAL-backed memtable + manifest record**

Replace `ForceFreezeMemtable` with:
```go
// ForceFreezeMemtable turns the active memtable immutable and installs a fresh
// WAL-backed one, recording the new memtable in the manifest.
func (s *Storage) ForceFreezeMemtable() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.memtable.IsEmpty() {
		return nil
	}
	if err := s.st.memtable.SyncWAL(); err != nil {
		return err
	}
	newID := s.allocID()
	mt, err := memtable.NewWithWAL(newID, s.walPath(newID), s.opts.SyncWrites)
	if err != nil {
		return err
	}
	old := s.st.memtable
	newImm := make([]*memtable.Memtable, 0, len(s.st.immMemtables)+1)
	newImm = append(newImm, old)
	newImm = append(newImm, s.st.immMemtables...)
	s.st = &state{
		memtable:     mt,
		immMemtables: newImm,
		l0:           s.st.l0,
		levels:       s.st.levels,
		sstables:     s.st.sstables,
	}
	return s.manifest.AddRecord(manifest.Record{Kind: manifest.KindNewMemtable, ID: newID})
}
```

- [ ] **Step 7: Update ForceFlushNextImmMemtable to reuse the memtable id and record the flush**

Replace `ForceFlushNextImmMemtable` with:
```go
// ForceFlushNextImmMemtable flushes the oldest immutable memtable to an L0 SST
// whose id is the memtable's id, records the flush, and deletes the WAL.
func (s *Storage) ForceFlushNextImmMemtable() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.st.immMemtables) == 0 {
		return fmt.Errorf("lsm: no immutable memtable to flush")
	}
	flushIdx := len(s.st.immMemtables) - 1
	target := s.st.immMemtables[flushIdx]
	id := target.ID()

	builder := sstable.NewBuilder(s.opts.BlockSize)
	it := target.Iter(nil, nil)
	for it.IsValid() {
		builder.Add(it.Key(), it.Value())
		it.Next()
	}
	sst, err := builder.Build(id, s.sstPath(id))
	if err != nil {
		return err
	}

	newImm := append([]*memtable.Memtable(nil), s.st.immMemtables[:flushIdx]...)
	newL0 := append([]int{id}, s.st.l0...)
	newSstables := cloneSstables(s.st.sstables)
	newSstables[id] = sst
	s.st = &state{
		memtable:     s.st.memtable,
		immMemtables: newImm,
		l0:           newL0,
		levels:       s.st.levels,
		sstables:     newSstables,
	}
	if err := s.manifest.AddRecord(manifest.Record{Kind: manifest.KindFlush, ID: id}); err != nil {
		return err
	}
	target.CloseWAL()
	os.Remove(target.WALPath())
	return nil
}
```

- [ ] **Step 8: Update Close to sync/close the WAL and manifest**

Replace `Close` with:
```go
// Close stops background compaction, syncs and closes the active WAL and
// manifest, and releases SST file handles.
func (s *Storage) Close() error {
	s.stopCompaction()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.memtable != nil {
		s.st.memtable.SyncWAL()
		s.st.memtable.CloseWAL()
	}
	for _, m := range s.st.immMemtables {
		m.CloseWAL()
	}
	if s.manifest != nil {
		s.manifest.Close()
	}
	for _, sst := range s.st.sstables {
		sst.Close()
	}
	return nil
}
```

- [ ] **Step 9: Record compaction in the manifest (compact.go)**

The manifest record must be written **while still holding `s.mu`**, so it serializes
with the freeze/flush manifest writes (which also hold `s.mu`). Writing it after
`s.mu.Unlock()` would race the background goroutine against a concurrent
freeze/flush on the shared manifest file handle.

In `internal/lsm/compact.go`, inside `runOnceCompaction`, locate the state swap
followed by the unlock:
```go
	s.st = &state{
		memtable:     s.st.memtable,
		immMemtables: s.st.immMemtables,
		l0:           newL0,
		levels:       newLevels,
		sstables:     newSstables,
	}
	s.mu.Unlock()

	// Close and delete superseded SSTs after the swap.
```
and replace it with (manifest write BEFORE the unlock; the comment about ordering
still holds because deletion happens after the unlock):
```go
	s.st = &state{
		memtable:     s.st.memtable,
		immMemtables: s.st.immMemtables,
		l0:           newL0,
		levels:       newLevels,
		sstables:     newSstables,
	}
	// Record the new layout before releasing the lock and before deleting old
	// files, so a crash mid-delete still recovers to the post-compaction state
	// and manifest writes stay serialized with freeze/flush.
	if err := s.manifest.AddRecord(manifest.Record{
		Kind:       manifest.KindCompaction,
		UpperLevel: task.UpperLevel,
		UpperIDs:   task.UpperIDs,
		LowerLevel: task.LowerLevel,
		LowerIDs:   task.LowerIDs,
		NewIDs:     newIDs,
	}); err != nil {
		s.mu.Unlock()
		return false, err
	}
	s.mu.Unlock()

	// Close and delete superseded SSTs after the swap.
```
Add `"mythdb/internal/manifest"` to the import block of `compact.go`:
```go
import (
	"fmt"
	"log"
	"os"
	"time"

	"mythdb/internal/iterator"
	"mythdb/internal/manifest"
	"mythdb/internal/sstable"
)
```

- [ ] **Step 10: Run tests to verify they pass**

Run: `go test ./internal/lsm/ && go test ./... && go vet ./... && go test -race ./internal/lsm/`
Expected: PASS everywhere; no data races. (Existing 2A lsm tests still pass — `Put`/`Delete`/freeze/flush keep their signatures; flush now writes `<id>.sst` reusing the memtable id, which the 2A tests do not assert against.)

- [ ] **Step 11: Commit**

```bash
git add internal/lsm/storage.go internal/lsm/compact.go internal/lsm/persist_test.go
git commit -m "feat: WriteBatch write path; persist manifest and WALs on freeze/flush/compaction"
```

---

## Task 6: Recovery on Open

**Files:**
- Create: `internal/lsm/recover.go`
- Modify: `internal/lsm/storage.go` (branch `Open` on an existing MANIFEST)
- Test: `internal/lsm/recover_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/lsm/recover_test.go`:
```go
package lsm

import (
	"fmt"
	"testing"
)

// reopen closes nothing (simulates a crash) and opens a new Storage on the same
// directory. Compaction is disabled so there is no background goroutine to race.
func TestRecoverUnflushedFromWAL(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Path: dir, BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := s.Put([]byte(fmt.Sprintf("key%03d", i)), []byte(fmt.Sprintf("val%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate a crash: do NOT call s.Close(). Reopen the same directory.

	s2, err := Open(Options{Path: dir, BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("key%03d", i)
		v, ok, err := s2.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if !ok || string(v) != fmt.Sprintf("val%03d", i) {
			t.Fatalf("after crash recovery, %q = %q ok=%v", k, v, ok)
		}
	}
}

func TestRecoverAfterFlush(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Path: dir, BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		s.Put([]byte(fmt.Sprintf("key%03d", i)), []byte(fmt.Sprintf("val%03d", i)))
	}
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()
	s.Close()

	s2, err := Open(Options{Path: dir, BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := len(s2.snapshot().l0); got != 1 {
		t.Fatalf("expected 1 recovered L0 SST, got %d", got)
	}
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("key%03d", i)
		v, ok, _ := s2.Get([]byte(k))
		if !ok || string(v) != fmt.Sprintf("val%03d", i) {
			t.Fatalf("after flush recovery, %q = %q ok=%v", k, v, ok)
		}
	}
}

func TestRecoverAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Path: dir, BlockSize: 256, TargetSSTSize: 1 << 20,
		Compaction: CompactionOptions{Strategy: "full"},
	}
	s, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	// Two flushed L0 SSTs.
	for i := 0; i < 10; i++ {
		s.Put([]byte(fmt.Sprintf("key%03d", i)), []byte("old"))
	}
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()
	for i := 0; i < 10; i++ {
		s.Put([]byte(fmt.Sprintf("key%03d", i)), []byte("new"))
	}
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()
	if _, err := s.runOnceCompaction(); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	st := s2.snapshot()
	if len(st.l0) != 0 {
		t.Fatalf("expected empty L0 after compaction recovery, got %d", len(st.l0))
	}
	if len(st.levels[len(st.levels)-1]) == 0 {
		t.Fatal("expected bottom level populated after recovery")
	}
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key%03d", i)
		v, ok, _ := s2.Get([]byte(k))
		if !ok || string(v) != "new" {
			t.Fatalf("after compaction recovery, %q = %q ok=%v", k, v, ok)
		}
	}
	// nextID must not collide with recovered ids.
	if err := s2.Put([]byte("zz"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s2.ForceFreezeMemtable(); err != nil {
		t.Fatal(err)
	}
	if err := s2.ForceFlushNextImmMemtable(); err != nil {
		t.Fatal(err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/lsm/ -run TestRecover`
Expected: FAIL — `Open` currently truncates the manifest (fresh start), so reopened data is missing (`TestRecoverUnflushedFromWAL` finds no keys).

- [ ] **Step 3: Write recover.go**

Create `internal/lsm/recover.go`:
```go
package lsm

import (
	"os"
	"sort"

	"mythdb/internal/manifest"
	"mythdb/internal/memtable"
	"mythdb/internal/sstable"
)

// removeInts returns src with every id in drop removed, preserving order.
func removeInts(src, drop []int) []int {
	if len(drop) == 0 {
		return src
	}
	dropSet := make(map[int]struct{}, len(drop))
	for _, id := range drop {
		dropSet[id] = struct{}{}
	}
	out := src[:0:0]
	for _, id := range src {
		if _, ok := dropSet[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

// recover rebuilds engine state from an existing manifest plus WALs.
func (s *Storage) recover(manifestPath string) error {
	records, man, err := manifest.Recover(manifestPath)
	if err != nil {
		return err
	}
	s.manifest = man

	numLevels := 0
	if s.controller != nil {
		numLevels = s.controller.NumLevels()
	}
	levels := make([][]int, numLevels)
	var l0 []int
	memSet := map[int]struct{}{}
	maxID := -1
	bump := func(id int) {
		if id > maxID {
			maxID = id
		}
	}

	for _, r := range records {
		switch r.Kind {
		case manifest.KindNewMemtable:
			memSet[r.ID] = struct{}{}
			bump(r.ID)
		case manifest.KindFlush:
			delete(memSet, r.ID)
			l0 = append([]int{r.ID}, l0...) // newest first, as flush prepends
			bump(r.ID)
		case manifest.KindCompaction:
			if r.UpperLevel == 0 {
				l0 = removeInts(l0, r.UpperIDs)
			} else {
				levels[r.UpperLevel-1] = removeInts(levels[r.UpperLevel-1], r.UpperIDs)
			}
			levels[r.LowerLevel-1] = removeInts(levels[r.LowerLevel-1], r.LowerIDs)
			levels[r.LowerLevel-1] = append([]int(nil), r.NewIDs...)
			for _, id := range r.NewIDs {
				bump(id)
			}
		}
	}

	// Open surviving SSTs.
	sstables := map[int]*sstable.SsTable{}
	openIDs := append([]int(nil), l0...)
	for _, lv := range levels {
		openIDs = append(openIDs, lv...)
	}
	for _, id := range openIDs {
		sst, err := sstable.Open(id, s.sstPath(id))
		if err != nil {
			return err
		}
		sstables[id] = sst
	}

	// Recover unflushed memtables (ascending id), newest-first in immMemtables.
	memIDs := make([]int, 0, len(memSet))
	for id := range memSet {
		memIDs = append(memIDs, id)
	}
	sort.Ints(memIDs)
	imm := make([]*memtable.Memtable, 0, len(memIDs))
	for i := len(memIDs) - 1; i >= 0; i-- {
		mt, err := memtable.RecoverWAL(memIDs[i], s.walPath(memIDs[i]), s.opts.SyncWrites)
		if err != nil {
			return err
		}
		imm = append(imm, mt)
	}

	// Fresh active memtable above all recovered ids.
	s.nextID = maxID + 1
	activeID := s.allocID()
	active, err := memtable.NewWithWAL(activeID, s.walPath(activeID), s.opts.SyncWrites)
	if err != nil {
		return err
	}
	if err := s.manifest.AddRecord(manifest.Record{Kind: manifest.KindNewMemtable, ID: activeID}); err != nil {
		return err
	}

	s.st = &state{
		memtable:     active,
		immMemtables: imm,
		l0:           l0,
		levels:       levels,
		sstables:     sstables,
	}
	return nil
}
```

- [ ] **Step 4: Branch Open on an existing MANIFEST**

In `internal/lsm/storage.go`, replace the fresh-start body of `Open` (everything from `if err := os.MkdirAll(...)` through the `s.manifest.AddRecord(... ID: 0 ...)` block) so it recovers when a manifest already exists. The new `Open`:
```go
func Open(opts Options) (*Storage, error) {
	if opts.BlockSize == 0 {
		opts.BlockSize = 4096
	}
	if opts.TargetSSTSize == 0 {
		opts.TargetSSTSize = 4 << 20
	}

	s := &Storage{opts: opts, nextID: 1}
	s.controller = buildController(opts.Compaction)

	if err := os.MkdirAll(opts.Path, 0o755); err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(opts.Path, "MANIFEST")

	if _, err := os.Stat(manifestPath); err == nil {
		if err := s.recover(manifestPath); err != nil {
			return nil, err
		}
	} else {
		man, err := manifest.Create(manifestPath)
		if err != nil {
			return nil, err
		}
		s.manifest = man
		mt, err := memtable.NewWithWAL(0, s.walPath(0), opts.SyncWrites)
		if err != nil {
			return nil, err
		}
		var levels [][]int
		if s.controller != nil {
			levels = make([][]int, s.controller.NumLevels())
		}
		s.st = &state{memtable: mt, levels: levels, sstables: map[int]*sstable.SsTable{}}
		if err := s.manifest.AddRecord(manifest.Record{Kind: manifest.KindNewMemtable, ID: 0}); err != nil {
			return nil, err
		}
	}

	if s.controller != nil && opts.Compaction.Interval > 0 {
		s.startCompaction(opts.Compaction.Interval)
	}
	return s, nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/lsm/ && go test ./... && go vet ./... && go test -race ./internal/lsm/`
Expected: PASS everywhere; no data races. All three recovery tests pass.

- [ ] **Step 6: Commit**

```bash
git add internal/lsm/recover.go internal/lsm/storage.go internal/lsm/recover_test.go
git commit -m "feat: recover engine state from manifest and WALs on Open"
```

---

## Self-Review Notes

- **Spec coverage:** WAL (Task 1), manifest (Task 2), SST meta CRC (Task 3), memtable WAL ownership (Task 4), WriteBatch + persistent freeze/flush/compaction (Task 5), recovery on Open (Task 6). All spec components map to a task.
- **Type consistency:** `wal.WAL{Create,Recover,Put,Sync,Close}` + `wal.Record{Key,Value}`; `manifest.Manifest{Create,Recover,AddRecord,Close}` + `manifest.Record{Kind,ID,UpperLevel,UpperIDs,LowerLevel,LowerIDs,NewIDs}` + `KindNewMemtable/KindFlush/KindCompaction`; `memtable.{NewWithWAL,RecoverWAL,SyncWAL,CloseWAL,WALPath}` and `Put` now returns `error`; engine `WriteBatch{Put,Delete}` + `Storage.Write`; helpers `walPath`, `removeInts`. The compaction manifest record fields match `compaction.Task` fields plus `NewIDs`.
- **Flush id reuse:** flush now writes `<memtableID>.sst` (was a freshly-allocated id in 2A); recovery relies on `Flush{id}` mapping memtable id → SST id. 2A tests do not assert SST ids, so they remain green.
- **Recovery correctness:** manifest records are folded with the same remove/replace semantics as the runtime `ApplyResult` (works for both full and leveled); unflushed memtables are replayed newest-first; `nextID = maxID+1` then a fresh active memtable is allocated above all recovered ids.
- **Crash-safety ordering:** WAL is write-through (visible after a crash without Close); manifest fsyncs each record; `Flush` is recorded after the SST is durably written; `Compaction` is recorded before old files are deleted.
- **Concurrency:** recovery tests disable compaction (no goroutine) for determinism; `go test -race` covers the engine. Manifest writes happen under the engine write lock (freeze/flush) or single-compactor path.
- **Deferred:** manifest compaction/snapshotting (unbounded growth) and orphan-file cleanup are out of scope (noted in spec); MVCC is Week 3.
```
