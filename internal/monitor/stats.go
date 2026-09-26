// Package monitor tracks per-peer network statistics used by the scheduler.
package monitor

import (
	"sync"
	"time"
)

// EWMA smoothing factors — higher α = faster adaptation.
const (
	alphaRTT        = 0.5 // RTT adapts quickly
	alphaThroughput = 0.3 // throughput is noisier, smooth more
	alphaLoss       = 0.2 // loss needs stability
)

// probe sliding window size for loss estimation
const probeWindowSize = 50

// NetworkStats is a copyable snapshot of one peer's measured conditions.
// Passed to the scorer as a value — no locks needed by the caller.
type NetworkStats struct {
	RTT        time.Duration // EWMA round-trip time
	Throughput float64       // Mbps, EWMA
	LossRate   float64       // 0.0–1.0, sliding window over last 50 probes
	Stability  float64       // 0.0–1.0, fraction of expected uptime since connect
	ActiveReqs int           // outstanding piece requests on this connection
}

// peerStats holds the live EWMA state for one peer.
// All fields are protected by mu.
type peerStats struct {
	mu          sync.Mutex
	rttMs       float64 // EWMA, milliseconds
	throughput  float64 // EWMA, Mbps
	lossRate    float64 // EWMA
	connectedAt time.Time
	activeReqs  int

	// circular probe window for loss estimation
	probeWindow [probeWindowSize]bool // true = ack received
	probeHead   int
	probeTotal  int // total probes recorded (capped at probeWindowSize)
}

func newPeerStats() *peerStats {
	return &peerStats{
		rttMs:       50.0, // 50 ms baseline
		throughput:  1.0,  // 1 Mbps baseline
		lossRate:    0.0,
		connectedAt: time.Now(),
	}
}

func (p *peerStats) snapshot() NetworkStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	uptime := time.Since(p.connectedAt)
	// stability = how consistently this peer has been connected
	// simple model: saturates to 1.0 after 60s of continuous connection
	stability := min64(uptime.Seconds()/60.0, 1.0)

	return NetworkStats{
		RTT:        time.Duration(p.rttMs * float64(time.Millisecond)),
		Throughput: p.throughput,
		LossRate:   p.lossRate,
		Stability:  stability,
		ActiveReqs: p.activeReqs,
	}
}

func (p *peerStats) recordRTT(rtt time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ms := float64(rtt.Milliseconds())
	p.rttMs = alphaRTT*ms + (1-alphaRTT)*p.rttMs
}

// recordTransfer updates throughput from an actual piece transfer.
// pieceBytes is the piece size; dur is the time from request send to data received.
func (p *peerStats) recordTransfer(pieceBytes int, dur time.Duration) {
	if dur <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	mbps := float64(pieceBytes) / (1024 * 1024) / dur.Seconds()
	p.throughput = alphaThroughput*mbps + (1-alphaThroughput)*p.throughput
}

// recordProbe records whether a probe was acknowledged.
func (p *peerStats) recordProbe(acked bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	idx := p.probeHead % probeWindowSize
	p.probeWindow[idx] = acked
	p.probeHead++
	if p.probeTotal < probeWindowSize {
		p.probeTotal++
	}

	// recompute loss rate from window
	if p.probeTotal == 0 {
		return
	}
	lost := 0
	for i := 0; i < p.probeTotal; i++ {
		if !p.probeWindow[i] {
			lost++
		}
	}
	sample := float64(lost) / float64(p.probeTotal)
	p.lossRate = alphaLoss*sample + (1-alphaLoss)*p.lossRate
}

func (p *peerStats) incActiveReqs() {
	p.mu.Lock()
	p.activeReqs++
	p.mu.Unlock()
}

func (p *peerStats) decActiveReqs() {
	p.mu.Lock()
	if p.activeReqs > 0 {
		p.activeReqs--
	}
	p.mu.Unlock()
}

// Map is a thread-safe store of per-peer network statistics.
type Map struct {
	mu    sync.RWMutex
	peers map[string]*peerStats
}

// NewMap returns an empty Map.
func NewMap() *Map {
	return &Map{peers: make(map[string]*peerStats)}
}

// Register adds a peer to the map with baseline stats.
// No-op if addr is already registered.
func (m *Map) Register(addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.peers[addr]; !ok {
		m.peers[addr] = newPeerStats()
	}
}

// Remove deletes a peer from the map.
func (m *Map) Remove(addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.peers, addr)
}

// Get returns a snapshot of current stats for addr.
// Returns baseline stats if addr is unknown.
func (m *Map) Get(addr string) NetworkStats {
	m.mu.RLock()
	p := m.peers[addr]
	m.mu.RUnlock()

	if p == nil {
		return NetworkStats{
			RTT:        50 * time.Millisecond,
			Throughput: 1.0,
			LossRate:   0.0,
			Stability:  0.5,
		}
	}
	return p.snapshot()
}

// RecordRTT updates the RTT EWMA for addr.
func (m *Map) RecordRTT(addr string, rtt time.Duration) {
	m.mu.RLock()
	p := m.peers[addr]
	m.mu.RUnlock()
	if p != nil {
		p.recordRTT(rtt)
	}
}

// RecordTransfer updates throughput for addr from a completed piece transfer.
func (m *Map) RecordTransfer(addr string, pieceBytes int, dur time.Duration) {
	m.mu.RLock()
	p := m.peers[addr]
	m.mu.RUnlock()
	if p != nil {
		p.recordTransfer(pieceBytes, dur)
	}
}

// RecordProbe records a probe outcome for addr.
func (m *Map) RecordProbe(addr string, acked bool) {
	m.mu.RLock()
	p := m.peers[addr]
	m.mu.RUnlock()
	if p != nil {
		p.recordProbe(acked)
	}
}

// IncActiveReqs increments the outstanding request counter for addr.
func (m *Map) IncActiveReqs(addr string) {
	m.mu.RLock()
	p := m.peers[addr]
	m.mu.RUnlock()
	if p != nil {
		p.incActiveReqs()
	}
}

// DecActiveReqs decrements the outstanding request counter for addr.
func (m *Map) DecActiveReqs(addr string) {
	m.mu.RLock()
	p := m.peers[addr]
	m.mu.RUnlock()
	if p != nil {
		p.decActiveReqs()
	}
}

func min64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
