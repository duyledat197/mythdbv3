package lsm

import "testing"

func TestWatermarkNoReaders(t *testing.T) {
	m := newMvcc(5)
	if m.watermark() != 5 {
		t.Fatalf("watermark with no readers = %d want 5 (latest ts)", m.watermark())
	}
}

func TestWatermarkTracksMinReader(t *testing.T) {
	m := newMvcc(10)
	m.addReader(7)
	m.addReader(3)
	m.addReader(7)
	if m.watermark() != 3 {
		t.Fatalf("watermark = %d want 3 (min open reader)", m.watermark())
	}
	m.removeReader(3)
	if m.watermark() != 7 {
		t.Fatalf("watermark after removing 3 = %d want 7", m.watermark())
	}
	m.removeReader(7) // one of two
	if m.watermark() != 7 {
		t.Fatalf("watermark still = %d want 7 (one reader at 7 remains)", m.watermark())
	}
	m.removeReader(7)
	if m.watermark() != 10 {
		t.Fatalf("watermark with no readers = %d want 10", m.watermark())
	}
}

func TestCommittedHistoryPrunes(t *testing.T) {
	m := newMvcc(0)
	m.recordCommitted(1, map[uint64]struct{}{100: {}})
	m.recordCommitted(2, map[uint64]struct{}{200: {}})
	m.recordCommitted(3, map[uint64]struct{}{300: {}})
	// With no open readers the watermark is the latest ts (3); everything <= 3
	// can be pruned.
	m.setTs(3)
	m.pruneCommitted()
	if n := m.committedCount(); n != 0 {
		t.Fatalf("expected all committed entries pruned, got %d", n)
	}
}

func TestConflictDetection(t *testing.T) {
	m := newMvcc(0)
	m.recordCommitted(5, map[uint64]struct{}{42: {}}) // a committer wrote key-hash 42 at ts 5
	// A reader that started at ts 4 and accessed key-hash 42 conflicts.
	if !m.hasConflict(4, map[uint64]struct{}{42: {}}) {
		t.Fatal("expected conflict: committer at ts 5 > readTs 4 wrote an accessed key")
	}
	// A reader that started at ts 5 (after that committer) does not conflict.
	if m.hasConflict(5, map[uint64]struct{}{42: {}}) {
		t.Fatal("did not expect conflict for readTs == committer ts")
	}
	// A reader accessing a different key does not conflict.
	if m.hasConflict(4, map[uint64]struct{}{99: {}}) {
		t.Fatal("did not expect conflict for disjoint access set")
	}
}
