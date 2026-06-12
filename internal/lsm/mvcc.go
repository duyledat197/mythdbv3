package lsm

import "sync"

// mvcc holds the monotonic commit-timestamp counter. Week 3B extends this into a
// full transaction controller (watermark, committed history); for 3A it is just
// the clock.
type mvcc struct {
	mu sync.Mutex
	ts uint64
}

func newMvcc(initial uint64) *mvcc { return &mvcc{ts: initial} }

// latestTs returns the latest committed timestamp.
func (m *mvcc) latestTs() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ts
}

// nextTs increments and returns the new commit timestamp.
func (m *mvcc) nextTs() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ts++
	return m.ts
}

// setTs raises the counter to at least ts (used during recovery).
func (m *mvcc) setTs(ts uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ts > m.ts {
		m.ts = ts
	}
}
