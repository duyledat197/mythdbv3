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
