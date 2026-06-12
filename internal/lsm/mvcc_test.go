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

func TestMVCCScanPrefixKeysOrderingAndBounds(t *testing.T) {
	s, err := Open(Options{Path: t.TempDir(), BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// "b" is a prefix of "bc": a naive bytes.Compare on encoded keys would misorder
	// these and drop "b" from a [.., "bc") scan.
	s.Put([]byte("a"), []byte("1"))
	s.Put([]byte("b"), []byte("2"))
	s.Put([]byte("bc"), []byte("3"))
	s.Put([]byte("c"), []byte("4"))

	collect := func(lower, upper []byte) []string {
		it, err := s.Scan(lower, upper)
		if err != nil {
			t.Fatal(err)
		}
		var ks []string
		for it.IsValid() {
			ks = append(ks, string(it.Key()))
			it.Next()
		}
		return ks
	}

	full := collect(nil, nil)
	wantFull := []string{"a", "b", "bc", "c"}
	if fmt.Sprint(full) != fmt.Sprint(wantFull) {
		t.Fatalf("full scan = %v want %v", full, wantFull)
	}
	// Exclusive upper "bc" must include "b" but exclude "bc".
	bounded := collect([]byte("a"), []byte("bc"))
	wantBounded := []string{"a", "b"}
	if fmt.Sprint(bounded) != fmt.Sprint(wantBounded) {
		t.Fatalf("scan [a, bc) = %v want %v", bounded, wantBounded)
	}
}
