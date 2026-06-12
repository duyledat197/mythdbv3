// Package lsm assembles memtables and SSTs into a read/write LSM engine.
package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
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

// Options configures a Storage instance.
type Options struct {
	Path          string // directory for SST files
	BlockSize     int    // target block size in bytes
	TargetSSTSize int64  // memtable freeze threshold AND target compaction SST size

	Compaction CompactionOptions

	SyncWrites bool // fsync each WAL write (durable but slow); default off
}

// CompactionOptions configures background compaction. Strategy "" disables it.
type CompactionOptions struct {
	Strategy            string        // "", "full", or "leveled"
	MaxLevels           int           // non-L0 levels (full defaults 1, leveled defaults 4)
	L0CompactionTrigger int           // leveled: L0 file count that triggers L0->L1 (default 4)
	LevelSizeMultiplier int           // leveled: size ratio between levels (default 10)
	BaseLevelSizeBytes  int64         // leveled: bottom-level base target (default 16 MiB)
	Interval            time.Duration // background tick; 0 disables the goroutine
}

// state is an immutable-by-convention snapshot of the engine's tiers. SSTs are
// referenced by id and resolved through sstables.
type state struct {
	memtable     *memtable.Memtable
	immMemtables []*memtable.Memtable     // newest first
	l0           []int                    // L0 SST ids, newest first
	levels       [][]int                  // levels[i] = ids of L(i+1), key-sorted, non-overlapping
	sstables     map[int]*sstable.SsTable // id -> open handle
}

// Storage is the LSM engine.
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

// Open initializes the engine. If a MANIFEST already exists in opts.Path the
// engine is recovered from it (manifest records + WALs); otherwise a fresh
// engine is created with a new manifest and first WAL.
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
		s.mvcc = newMvcc(0)
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

// buildController constructs the compaction controller for the given options,
// or nil when compaction is disabled.
func buildController(c CompactionOptions) compaction.Controller {
	switch c.Strategy {
	case "full":
		levels := c.MaxLevels
		if levels == 0 {
			levels = 1
		}
		return &compaction.Full{MaxLevels: levels}
	case "leveled":
		return newLeveledController(c)
	default:
		return nil
	}
}

// newLeveledController builds a leveled controller, filling in defaults.
func newLeveledController(c CompactionOptions) compaction.Controller {
	maxLevels := c.MaxLevels
	if maxLevels == 0 {
		maxLevels = 4
	}
	trigger := c.L0CompactionTrigger
	if trigger == 0 {
		trigger = 4
	}
	mult := c.LevelSizeMultiplier
	if mult == 0 {
		mult = 10
	}
	base := c.BaseLevelSizeBytes
	if base == 0 {
		base = 16 << 20
	}
	return &compaction.Leveled{
		MaxLevels:  maxLevels,
		L0Trigger:  trigger,
		Multiplier: mult,
		BaseBytes:  base,
	}
}

func (s *Storage) snapshot() *state {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.st
}

// allocID hands out a unique, monotonically increasing id for memtables and SSTs.
// It uses a dedicated idMu so it can be called either with or without the main
// mu held. Lock ordering is always mu -> idMu (never the reverse); callers must
// not acquire mu while already holding idMu.
func (s *Storage) allocID() int {
	s.idMu.Lock()
	defer s.idMu.Unlock()
	id := s.nextID
	s.nextID++
	return id
}

func (s *Storage) sstPath(id int) string {
	return filepath.Join(s.opts.Path, fmt.Sprintf("%05d.sst", id))
}

func (s *Storage) walPath(id int) string {
	return filepath.Join(s.opts.Path, fmt.Sprintf("%05d.wal", id))
}

func cloneSstables(m map[int]*sstable.SsTable) map[int]*sstable.SsTable {
	n := make(map[int]*sstable.SsTable, len(m))
	for k, v := range m {
		n[k] = v
	}
	return n
}

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

// writeEncodedBatch applies entries (encoded keys) to the active memtable under
// the engine lock, freezing if the memtable is full.
func (s *Storage) writeEncodedBatch(entries []memtable.Entry) error {
	s.mu.Lock()
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

// Write applies a batch atomically at a fresh commit timestamp. It serializes
// with transaction commits and records its write set so open transactions detect
// conflicts against it.
func (s *Storage) Write(b *WriteBatch) error {
	s.mvcc.commitMu.Lock()
	defer s.mvcc.commitMu.Unlock()
	commitTs := s.mvcc.nextTs()
	entries := make([]memtable.Entry, len(b.ops))
	writeSet := make(map[uint64]struct{}, len(b.ops))
	for i, op := range b.ops {
		entries[i] = memtable.Entry{Key: key.Encode(op.key, commitTs), Value: op.value}
		writeSet[hashKey(op.key)] = struct{}{}
	}
	if err := s.writeEncodedBatch(entries); err != nil {
		return err
	}
	s.mvcc.recordCommitted(commitTs, writeSet)
	s.mvcc.pruneCommitted()
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

// hashKey hashes a user key for transaction conflict tracking.
func hashKey(userKey []byte) uint64 {
	h := fnv.New64a()
	h.Write(userKey)
	return h.Sum64()
}

func seekSST(sst *sstable.SsTable, lower []byte) (iterator.StorageIterator, error) {
	if lower == nil {
		return sstable.NewIterAndSeekToFirst(sst)
	}
	return sstable.NewIterAndSeekToKey(sst, lower)
}

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

// Begin starts a transaction reading a consistent snapshot at the latest
// committed timestamp.
func (s *Storage) Begin() *Txn {
	readTs := s.mvcc.latestTs()
	s.mvcc.addReader(readTs)
	return &Txn{
		engine:    s,
		readTs:    readTs,
		local:     map[string][]byte{},
		accessSet: map[uint64]struct{}{},
	}
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

// Close stops background compaction, syncs and closes the active WAL and
// manifest, and releases SST file handles.
func (s *Storage) Close() error {
	s.stopCompaction()
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	if s.st.memtable != nil {
		errs = append(errs, s.st.memtable.SyncWAL(), s.st.memtable.CloseWAL())
	}
	for _, m := range s.st.immMemtables {
		errs = append(errs, m.CloseWAL())
	}
	if s.manifest != nil {
		errs = append(errs, s.manifest.Close())
	}
	for _, sst := range s.st.sstables {
		errs = append(errs, sst.Close())
	}
	return errors.Join(errs...)
}
