package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRecoverWriteBatchAtomic confirms a multi-op batch recovers in full after a
// crash (no Close): every op of the batch is present.
func TestRecoverWriteBatchAtomic(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Path: dir, BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	b := &WriteBatch{}
	for i := 0; i < 8; i++ {
		b.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)))
	}
	b.Delete([]byte("k03"))
	if err := s.Write(b); err != nil {
		t.Fatal(err)
	}
	// Crash: no Close. Reopen and verify the whole batch recovered.

	s2, err := Open(Options{Path: dir, BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("k%02d", i)
		v, ok, _ := s2.Get([]byte(k))
		if k == "k03" {
			if ok {
				t.Fatalf("k03 should be deleted after recovery")
			}
			continue
		}
		if !ok || string(v) != fmt.Sprintf("v%02d", i) {
			t.Fatalf("batch recovery %q = %q ok=%v", k, v, ok)
		}
	}
}

// TestRecoverDeletesOrphanWAL confirms repeated reopen without flush does not
// accumulate WAL files: each recovery cleans up the previous active WAL once its
// (empty) memtable is dropped.
func TestRecoverDeletesOrphanWAL(t *testing.T) {
	dir := t.TempDir()
	countWALs := func() int {
		entries, _ := os.ReadDir(dir)
		n := 0
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".wal") {
				n++
			}
		}
		return n
	}
	// First session writes data, then closes cleanly.
	s, _ := Open(Options{Path: dir, BlockSize: 4096, TargetSSTSize: 1 << 20})
	s.Put([]byte("a"), []byte("1"))
	s.Close()

	// Reopen several times without writing; the empty active WAL from each prior
	// session must be cleaned up, so the WAL count stays bounded.
	for i := 0; i < 4; i++ {
		s2, err := Open(Options{Path: dir, BlockSize: 4096, TargetSSTSize: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		s2.Close()
	}
	// The data memtable (id 0) WAL plus at most the current active WAL should
	// remain — never a growing pile.
	if n := countWALs(); n > 2 {
		entries, _ := os.ReadDir(dir)
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("orphan WALs accumulated: %d wal files (%v)", n, filepath.Base(dir)+": "+strings.Join(names, ","))
	}
}

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
