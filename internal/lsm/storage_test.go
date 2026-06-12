package lsm

import (
	"fmt"
	"testing"
)

func newStorage(t *testing.T) *Storage {
	t.Helper()
	s, err := Open(Options{
		Path:          t.TempDir(),
		BlockSize:     4096,
		TargetSSTSize: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPutGetDelete(t *testing.T) {
	s := newStorage(t)
	if err := s.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	v, ok, err := s.Get([]byte("a"))
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("get a -> %q ok=%v err=%v", v, ok, err)
	}
	if err := s.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Get([]byte("a")); ok {
		t.Fatal("a should be deleted")
	}
}

func TestGetAfterFreezeAndFlush(t *testing.T) {
	s := newStorage(t)
	for i := 0; i < 50; i++ {
		s.Put([]byte(fmt.Sprintf("key%03d", i)), []byte(fmt.Sprintf("val%03d", i)))
	}
	if err := s.ForceFreezeMemtable(); err != nil {
		t.Fatal(err)
	}
	if err := s.ForceFlushNextImmMemtable(); err != nil {
		t.Fatal(err)
	}
	v, ok, err := s.Get([]byte("key025"))
	if err != nil || !ok || string(v) != "val025" {
		t.Fatalf("after flush key025 -> %q ok=%v err=%v", v, ok, err)
	}
}

func TestOverwriteAcrossTiers(t *testing.T) {
	s := newStorage(t)
	s.Put([]byte("k"), []byte("old"))
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()
	s.Put([]byte("k"), []byte("new"))
	v, ok, _ := s.Get([]byte("k"))
	if !ok || string(v) != "new" {
		t.Fatalf("want new got %q ok=%v", v, ok)
	}
}

func TestDeleteAcrossTiers(t *testing.T) {
	s := newStorage(t)
	s.Put([]byte("k"), []byte("v"))
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()
	s.Delete([]byte("k"))
	if _, ok, _ := s.Get([]byte("k")); ok {
		t.Fatal("k should read as deleted")
	}
}

func TestScanMergesTiers(t *testing.T) {
	s := newStorage(t)
	s.Put([]byte("a"), []byte("1"))
	s.Put([]byte("b"), []byte("1"))
	s.Put([]byte("c"), []byte("1"))
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()
	s.Put([]byte("b"), []byte("2"))
	s.Delete([]byte("c"))
	s.Put([]byte("d"), []byte("2"))

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
	want := []kv{{"a", "1"}, {"b", "2"}, {"d", "2"}}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at %d got %v want %v", i, got[i], want[i])
		}
	}
}

func TestScanBounded(t *testing.T) {
	s := newStorage(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		s.Put([]byte(k), []byte("v"))
	}
	it, err := s.Scan([]byte("b"), []byte("d"))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for it.IsValid() {
		got = append(got, string(it.Key()))
		it.Next()
	}
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("bounded scan got %v", got)
	}
}
