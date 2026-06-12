package lsm

import (
	"fmt"
	"testing"
)

// liveVersionCount counts total stored versions of userKey across all SSTs in
// the current state (L0 + all levels). Used to assert GC happened.
func liveVersionCount(t *testing.T, s *Storage, userKey string) int {
	t.Helper()
	st := s.snapshot()
	count := 0
	// Count versions across L0 and levels by scanning each SST.
	for _, id := range st.l0 {
		count += countVersionsInSST(t, s, id, userKey)
	}
	for _, lvl := range st.levels {
		for _, id := range lvl {
			count += countVersionsInSST(t, s, id, userKey)
		}
	}
	return count
}

func TestGCCollapsesVersionsNoReaders(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{
		Path: dir, BlockSize: 256, TargetSSTSize: 1 << 20,
		Compaction: CompactionOptions{Strategy: "full"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Five versions of "k" across two SSTs.
	for i := 0; i < 3; i++ {
		s.Put([]byte("k"), []byte(fmt.Sprintf("v%d", i)))
	}
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()
	for i := 3; i < 5; i++ {
		s.Put([]byte("k"), []byte(fmt.Sprintf("v%d", i)))
	}
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()

	// No open transactions -> watermark = latest -> GC collapses to one version.
	if _, err := s.runOnceCompaction(); err != nil {
		t.Fatal(err)
	}
	if n := liveVersionCount(t, s, "k"); n != 1 {
		t.Fatalf("expected 1 retained version after GC, got %d", n)
	}
	if v, ok, _ := s.Get([]byte("k")); !ok || string(v) != "v4" {
		t.Fatalf("newest value must survive GC: %q ok=%v", v, ok)
	}
}

func TestGCKeepsVersionsForOpenReader(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{
		Path: dir, BlockSize: 256, TargetSSTSize: 1 << 20,
		Compaction: CompactionOptions{Strategy: "full"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Put([]byte("k"), []byte("v0"))
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()

	// Open a reader at this snapshot, then write more versions.
	txn := s.Begin()
	for i := 1; i < 4; i++ {
		s.Put([]byte("k"), []byte(fmt.Sprintf("v%d", i)))
	}
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()

	// Watermark is held down by the open reader, so the version it can see (v0)
	// must survive compaction.
	if _, err := s.runOnceCompaction(); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := txn.Get([]byte("k")); !ok || string(v) != "v0" {
		t.Fatalf("open reader must still see v0 after GC: %q ok=%v", v, ok)
	}
	txn.Rollback()
}

func TestGCReclaimsTombstone(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{
		Path: dir, BlockSize: 256, TargetSSTSize: 1 << 20,
		Compaction: CompactionOptions{Strategy: "full"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Put([]byte("k"), []byte("v"))
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()
	s.Delete([]byte("k"))
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()

	if _, err := s.runOnceCompaction(); err != nil {
		t.Fatal(err)
	}
	// With no readers, the deleted key and its tombstone are fully reclaimed.
	if n := liveVersionCount(t, s, "k"); n != 0 {
		t.Fatalf("expected tombstone and value reclaimed, got %d versions", n)
	}
	if _, ok, _ := s.Get([]byte("k")); ok {
		t.Fatal("k must read as absent after tombstone GC")
	}
}
