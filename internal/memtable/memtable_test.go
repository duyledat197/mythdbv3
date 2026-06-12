package memtable

import "testing"

func TestPutGet(t *testing.T) {
	m := New(0)
	m.Put([]byte("k1"), []byte("v1"))
	m.Put([]byte("k2"), []byte("v2"))
	if v, ok := m.Get([]byte("k1")); !ok || string(v) != "v1" {
		t.Fatalf("k1=%q ok=%v", v, ok)
	}
	if _, ok := m.Get([]byte("missing")); ok {
		t.Fatal("missing should not be found")
	}
}

func TestPutOverwrite(t *testing.T) {
	m := New(0)
	m.Put([]byte("k"), []byte("old"))
	m.Put([]byte("k"), []byte("new"))
	if v, _ := m.Get([]byte("k")); string(v) != "new" {
		t.Fatalf("want new got %q", v)
	}
}

func TestTombstone(t *testing.T) {
	m := New(0)
	m.Put([]byte("k"), []byte{}) // tombstone
	v, ok := m.Get([]byte("k"))
	if !ok || len(v) != 0 {
		t.Fatalf("tombstone present with empty value; got ok=%v v=%q", ok, v)
	}
}

func TestIteratorOrderedAndBounded(t *testing.T) {
	m := New(0)
	for _, k := range []string{"d", "a", "c", "b", "e"} {
		m.Put([]byte(k), []byte("v"+k))
	}
	it := m.Iter([]byte("b"), []byte("e")) // [b, e) -> b, c, d
	var got []string
	for it.IsValid() {
		got = append(got, string(it.Key()))
		it.Next()
	}
	want := []string{"b", "c", "d"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at %d got %v want %v", i, got[i], want[i])
		}
	}
}

func TestIteratorUnbounded(t *testing.T) {
	m := New(0)
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))
	it := m.Iter(nil, nil)
	var n int
	for it.IsValid() {
		n++
		it.Next()
	}
	if n != 2 {
		t.Fatalf("want 2 got %d", n)
	}
}

func TestApproximateSizeGrows(t *testing.T) {
	m := New(0)
	if m.ApproximateSize() != 0 {
		t.Fatal("empty size should be 0")
	}
	m.Put([]byte("k"), []byte("v"))
	if m.ApproximateSize() == 0 {
		t.Fatal("size should grow after put")
	}
}
