package block

import "testing"

func buildBlock(t *testing.T, pairs [][2]string) *Block {
	t.Helper()
	b := NewBuilder(4096)
	for _, p := range pairs {
		if !b.Add([]byte(p[0]), []byte(p[1])) {
			t.Fatalf("unexpected full block adding %q", p[0])
		}
	}
	return b.Build()
}

func TestBlockEncodeDecodeRoundTrip(t *testing.T) {
	blk := buildBlock(t, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})
	raw := blk.Encode()
	got, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	it := NewIterAndSeekToFirst(got)
	var keys []string
	for it.IsValid() {
		keys = append(keys, string(it.Key()))
		it.Next()
	}
	if len(keys) != 3 || keys[0] != "a" || keys[2] != "c" {
		t.Fatalf("round trip keys=%v", keys)
	}
}

func TestBlockDecodeCorruptChecksum(t *testing.T) {
	blk := buildBlock(t, [][2]string{{"a", "1"}})
	raw := blk.Encode()
	raw[0] ^= 0xff // corrupt
	if _, err := Decode(raw); err == nil {
		t.Fatal("expected checksum error")
	}
}

func TestBuilderRespectsBlockSize(t *testing.T) {
	b := NewBuilder(20) // tiny
	if !b.Add([]byte("aaaa"), []byte("bbbb")) {
		t.Fatal("first add must always succeed")
	}
	if b.Add([]byte("cccc"), []byte("dddd")) {
		t.Fatal("expected block full")
	}
}

func TestSeekToKey(t *testing.T) {
	blk := buildBlock(t, [][2]string{{"a", "1"}, {"c", "3"}, {"e", "5"}})
	it := NewIterAndSeekToKey(blk, []byte("b")) // first key >= b is c
	if !it.IsValid() || string(it.Key()) != "c" {
		t.Fatalf("seek b -> %q valid=%v", it.Key(), it.IsValid())
	}
	it = NewIterAndSeekToKey(blk, []byte("e"))
	if !it.IsValid() || string(it.Value()) != "5" {
		t.Fatalf("seek e -> %q", it.Value())
	}
	it = NewIterAndSeekToKey(blk, []byte("z"))
	if it.IsValid() {
		t.Fatal("seek past end should be invalid")
	}
}
