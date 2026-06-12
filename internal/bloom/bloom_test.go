package bloom

import (
	"fmt"
	"testing"
)

func TestNoFalseNegatives(t *testing.T) {
	var hashes []uint32
	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		hashes = append(hashes, Hash(keys[i]))
	}
	b := Build(hashes, BitsPerKey(len(hashes), 0.01))
	for _, k := range keys {
		if !b.MayContain(Hash(k)) {
			t.Fatalf("false negative for %q", k)
		}
	}
}

func TestEncodeDecode(t *testing.T) {
	hashes := []uint32{Hash([]byte("a")), Hash([]byte("b"))}
	b := Build(hashes, 10)
	got, err := Decode(b.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if !got.MayContain(Hash([]byte("a"))) {
		t.Fatal("decoded filter lost a present key")
	}
}

func TestFalsePositiveRateReasonable(t *testing.T) {
	var hashes []uint32
	for i := 0; i < 10000; i++ {
		hashes = append(hashes, Hash([]byte(fmt.Sprintf("present-%d", i))))
	}
	b := Build(hashes, BitsPerKey(len(hashes), 0.01))
	fp := 0
	trials := 10000
	for i := 0; i < trials; i++ {
		if b.MayContain(Hash([]byte(fmt.Sprintf("absent-%d", i)))) {
			fp++
		}
	}
	rate := float64(fp) / float64(trials)
	if rate > 0.05 {
		t.Fatalf("false positive rate too high: %f", rate)
	}
}
