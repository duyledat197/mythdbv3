package lsm

import (
	"errors"
	"testing"
)

func TestTxnCommitApplies(t *testing.T) {
	s := newTxnStorage(t)
	txn := s.Begin()
	txn.Put([]byte("a"), []byte("1"))
	txn.Put([]byte("b"), []byte("2"))
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := s.Get([]byte("a")); !ok || string(v) != "1" {
		t.Fatalf("committed a = %q ok=%v", v, ok)
	}
	if v, ok, _ := s.Get([]byte("b")); !ok || string(v) != "2" {
		t.Fatalf("committed b = %q ok=%v", v, ok)
	}
}

func TestTxnWriteWriteConflictAborts(t *testing.T) {
	s := newTxnStorage(t)
	t1 := s.Begin()
	t2 := s.Begin()
	t1.Put([]byte("k"), []byte("t1"))
	t2.Put([]byte("k"), []byte("t2"))
	if err := t1.Commit(); err != nil {
		t.Fatalf("t1 commit should succeed: %v", err)
	}
	// t2 wrote the same key and started before t1 committed -> conflict.
	if err := t2.Commit(); !errors.Is(err, ErrSerialization) {
		t.Fatalf("t2 commit should abort with ErrSerialization, got %v", err)
	}
	if v, _, _ := s.Get([]byte("k")); string(v) != "t1" {
		t.Fatalf("k should be t1 (t2 aborted), got %q", v)
	}
}

func TestTxnReadWriteConflictAborts(t *testing.T) {
	s := newTxnStorage(t)
	s.Put([]byte("k"), []byte("v0"))
	t1 := s.Begin()
	// t1 reads k, then plans to write l based on it.
	if _, _, err := t1.Get([]byte("k")); err != nil {
		t.Fatal(err)
	}
	t1.Put([]byte("l"), []byte("derived"))

	// t2 writes k and commits before t1.
	t2 := s.Begin()
	t2.Put([]byte("k"), []byte("v1"))
	if err := t2.Commit(); err != nil {
		t.Fatal(err)
	}
	// t1's read set (k) intersects t2's write set (k) -> abort.
	if err := t1.Commit(); !errors.Is(err, ErrSerialization) {
		t.Fatalf("t1 commit should abort (read-write conflict), got %v", err)
	}
}

func TestTxnNonConflictingCommit(t *testing.T) {
	s := newTxnStorage(t)
	t1 := s.Begin()
	t2 := s.Begin()
	t1.Put([]byte("a"), []byte("1"))
	t2.Put([]byte("b"), []byte("2"))
	if err := t1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := t2.Commit(); err != nil {
		t.Fatalf("disjoint txn should commit, got %v", err)
	}
}
