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
