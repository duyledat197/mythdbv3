package lsm

import (
	"testing"
)

func newTxnStorage(t *testing.T) *Storage {
	t.Helper()
	s, err := Open(Options{Path: t.TempDir(), BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestTxnReadsOwnWrites(t *testing.T) {
	s := newTxnStorage(t)
	txn := s.Begin()
	txn.Put([]byte("a"), []byte("1"))
	txn.Delete([]byte("b"))
	if v, ok, _ := txn.Get([]byte("a")); !ok || string(v) != "1" {
		t.Fatalf("txn should see its own put: %q ok=%v", v, ok)
	}
	if _, ok, _ := txn.Get([]byte("b")); ok {
		t.Fatal("txn should see its own delete")
	}
	// Not yet committed: the engine does not see the write.
	if _, ok, _ := s.Get([]byte("a")); ok {
		t.Fatal("engine should not see uncommitted txn write")
	}
}

func TestTxnSnapshotIsolation(t *testing.T) {
	s := newTxnStorage(t)
	s.Put([]byte("k"), []byte("v0"))
	txn := s.Begin() // snapshot at this point
	// A concurrent write after the snapshot must NOT be visible to txn.
	s.Put([]byte("k"), []byte("v1"))
	if v, ok, _ := txn.Get([]byte("k")); !ok || string(v) != "v0" {
		t.Fatalf("txn snapshot should see v0, got %q ok=%v", v, ok)
	}
}

func TestTxnScanMergesLocalAndSnapshot(t *testing.T) {
	s := newTxnStorage(t)
	s.Put([]byte("a"), []byte("a0"))
	s.Put([]byte("b"), []byte("b0"))
	s.Put([]byte("c"), []byte("c0"))
	txn := s.Begin()
	txn.Put([]byte("b"), []byte("bX")) // override
	txn.Delete([]byte("c"))            // hide
	txn.Put([]byte("d"), []byte("dX")) // new

	it, err := txn.Scan(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	type kv struct{ k, v string }
	var got []kv
	for it.IsValid() {
		got = append(got, kv{string(it.Key()), string(it.Value())})
		it.Next()
	}
	want := []kv{{"a", "a0"}, {"b", "bX"}, {"d", "dX"}}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at %d got %v want %v", i, got[i], want[i])
		}
	}
}

func TestTxnRollbackDiscards(t *testing.T) {
	s := newTxnStorage(t)
	txn := s.Begin()
	txn.Put([]byte("a"), []byte("1"))
	txn.Rollback()
	if _, ok, _ := s.Get([]byte("a")); ok {
		t.Fatal("rolled-back write must not appear in the engine")
	}
	// Watermark returns to latest once the reader is gone.
	if s.mvcc.watermark() != s.mvcc.latestTs() {
		t.Fatal("watermark should equal latest ts after rollback")
	}
}
