package key

import "testing"

func TestEncodeRoundTrip(t *testing.T) {
	enc := Encode([]byte("hello"), 42)
	if string(UserKey(enc)) != "hello" {
		t.Fatalf("user key = %q", UserKey(enc))
	}
	if Timestamp(enc) != 42 {
		t.Fatalf("ts = %d", Timestamp(enc))
	}
}

func TestOrderingNewestFirstSameUserKey(t *testing.T) {
	// Same user key: higher timestamp must sort BEFORE lower timestamp.
	a := Encode([]byte("k"), 10)
	b := Encode([]byte("k"), 5)
	if Compare(a, b) >= 0 {
		t.Fatalf("expected k@10 < k@5 in sort order")
	}
}

func TestOrderingByUserKeyFirst(t *testing.T) {
	// Different user keys order by user key regardless of timestamp.
	a := Encode([]byte("a"), 1)   // small ts
	b := Encode([]byte("b"), 999) // large ts
	if Compare(a, b) >= 0 {
		t.Fatalf("expected a@1 < b@999")
	}
}

func TestCompareUserKey(t *testing.T) {
	a := Encode([]byte("a"), 7)
	b := Encode([]byte("a"), 99)
	if CompareUserKey(a, b) != 0 {
		t.Fatalf("same user key should compare equal")
	}
	c := Encode([]byte("b"), 1)
	if CompareUserKey(a, c) >= 0 {
		t.Fatalf("a < b by user key")
	}
}

func TestRangeBeginIsNewest(t *testing.T) {
	// Encode(k, TsRangeBegin) must sort at or before any real version of k.
	begin := Encode([]byte("k"), TsRangeBegin)
	real := Encode([]byte("k"), 1000)
	if Compare(begin, real) > 0 {
		t.Fatalf("range-begin should be <= any real version")
	}
}

func TestPrefixUserKeyOrdering(t *testing.T) {
	// A shorter user key that is a prefix of a longer one must still order before
	// it, regardless of the timestamp suffix bytes.
	short := Encode([]byte("b"), 1)
	long := Encode([]byte("bc"), 1)
	if Compare(short, long) >= 0 {
		t.Fatalf("expected b@1 < bc@1 (user key ordering), got %d", Compare(short, long))
	}
	// The exclusive upper bound Encode("bc", TsRangeBegin) must sort AFTER every
	// version of "b" (so a scan [.., "bc") still includes "b")...
	upper := Encode([]byte("bc"), TsRangeBegin)
	if Compare(short, upper) >= 0 {
		t.Fatalf("expected b@1 < upper-bound(bc), got %d", Compare(short, upper))
	}
	// ...while every version of "bc" sorts at/after that bound (so it is excluded).
	bcVersion := Encode([]byte("bc"), 5)
	if Compare(bcVersion, upper) < 0 {
		t.Fatalf("expected bc@5 >= upper-bound(bc) to be excluded, got %d", Compare(bcVersion, upper))
	}
}
