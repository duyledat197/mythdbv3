package lsm

import (
	"fmt"
	"testing"
)

func newFullStorage(t *testing.T) *Storage {
	t.Helper()
	s, err := Open(Options{
		Path:          t.TempDir(),
		BlockSize:     256,
		TargetSSTSize: 1 << 20,
		Compaction:    CompactionOptions{Strategy: "full"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// flushAll freezes and flushes the active memtable into an L0 SST.
func flushAll(t *testing.T, s *Storage) {
	t.Helper()
	if err := s.ForceFreezeMemtable(); err != nil {
		t.Fatal(err)
	}
	if err := s.ForceFlushNextImmMemtable(); err != nil {
		t.Fatal(err)
	}
}

func TestFullCompactionCollapsesL0(t *testing.T) {
	s := newFullStorage(t)
	// Two L0 SSTs where the second overwrites and deletes keys from the first.
	s.Put([]byte("a"), []byte("1"))
	s.Put([]byte("b"), []byte("1"))
	s.Put([]byte("c"), []byte("1"))
	flushAll(t, s)
	s.Put([]byte("b"), []byte("2")) // overwrite
	s.Delete([]byte("c"))           // tombstone
	s.Put([]byte("d"), []byte("2"))
	flushAll(t, s)

	// Before compaction: 2 L0 SSTs.
	if got := len(s.snapshot().l0); got != 2 {
		t.Fatalf("expected 2 L0 SSTs, got %d", got)
	}

	did, err := s.runOnceCompaction()
	if err != nil {
		t.Fatal(err)
	}
	if !did {
		t.Fatal("expected compaction to run")
	}

	st := s.snapshot()
	if len(st.l0) != 0 {
		t.Fatalf("L0 should be empty after full compaction, got %d", len(st.l0))
	}
	// Bottom level (levels[0] for full with MaxLevels=1) holds the merged run.
	if len(st.levels[len(st.levels)-1]) == 0 {
		t.Fatal("bottom level should hold merged SSTs")
	}

	// Correctness: newest values win, tombstoned key is gone.
	check := func(k, want string, wantOK bool) {
		v, ok, err := s.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if ok != wantOK || (wantOK && string(v) != want) {
			t.Fatalf("get %q -> %q ok=%v (want %q ok=%v)", k, v, ok, want, wantOK)
		}
	}
	check("a", "1", true)
	check("b", "2", true)
	check("c", "", false) // tombstone dropped
	check("d", "2", true)

	// A second compaction with nothing new is a no-op.
	did, err = s.runOnceCompaction()
	if err != nil {
		t.Fatal(err)
	}
	if did {
		t.Fatal("expected no-op on second compaction")
	}
}

func TestFullCompactionDropsTombstones(t *testing.T) {
	s := newFullStorage(t)
	for i := 0; i < 20; i++ {
		s.Put([]byte(fmt.Sprintf("key%03d", i)), []byte("v"))
	}
	flushAll(t, s)
	for i := 0; i < 20; i++ {
		s.Delete([]byte(fmt.Sprintf("key%03d", i)))
	}
	flushAll(t, s)

	if _, err := s.runOnceCompaction(); err != nil {
		t.Fatal(err)
	}

	// After dropping tombstones at the bottom level, a full scan yields nothing.
	it, err := s.Scan(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if it.IsValid() {
		t.Fatalf("expected empty scan, first key=%q", it.Key())
	}
}
