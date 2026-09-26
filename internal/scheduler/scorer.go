// Package scheduler implements network-aware peer selection for SwarmFS.
//
// The scorer assigns each peer a score in [0, 1] based on observed network
// conditions. The selector picks the best-scoring peer for a given piece.
//
// This is the research contribution: instead of "who has the piece?" SwarmFS
// asks "who should I get this piece from right now?"
package scheduler

import (
	"math"

	"github.com/manoj-1407/swarmfs/internal/monitor"
)

// Weights controls the relative importance of each network metric.
// All weights should sum to 1.0 (they are normalised internally if not).
type Weights struct {
	Throughput float64 // reward high bandwidth
	RTT        float64 // penalise high latency
	Loss       float64 // penalise packet loss
	Stability  float64 // reward long-lived connections
	Load       float64 // penalise heavily-loaded peers
}

// Default is the recommended starting weight set.
// Tuned for balanced LAN/WAN performance.
var Default = Weights{
	Throughput: 0.35,
	RTT:        0.30,
	Loss:       0.25,
	Stability:  0.05,
	Load:       0.05,
}

// baselines for normalising raw measurements into [0, 1]
const (
	rttBaselineMS    = 50.0  // ms — anything under this scores full RTT marks
	tputBaselineMbps = 10.0  // Mbps — reference throughput for normalisation
	maxActiveReqs    = 4     // per-peer concurrency cap used for load penalty
)

// Score returns a value in approximately [0, 1] for stats under weights w.
// Higher is better. Pure function — no side effects, no I/O.
//
// Formula:
//
//	score = w.Throughput * tputNorm
//	      + w.RTT        * rttNorm
//	      + w.Loss       * (1 - lossRate)
//	      + w.Stability  * stability
//	      - w.Load       * loadPenalty
func Score(stats monitor.NetworkStats, w Weights) float64 {
	// throughput: 0 → 0, baseline → 1, above baseline capped at 1
	tputNorm := math.Min(stats.Throughput/tputBaselineMbps, 1.0)

	// RTT: 0ms → 1 (perfect), high ms → near 0
	rttNorm := 1.0 / (1.0 + stats.RTT.Seconds()*1000.0/rttBaselineMS)

	// loss: 0% loss → 1, 100% loss → 0
	lossScore := 1.0 - math.Max(0, math.Min(1, stats.LossRate))

	// stability: already 0–1 from monitor
	stab := math.Max(0, math.Min(1, stats.Stability))

	// load: 0 active reqs → 0 penalty, maxActiveReqs → full penalty
	load := float64(stats.ActiveReqs) / float64(maxActiveReqs)
	load = math.Min(load, 1.0)

	return w.Throughput*tputNorm +
		w.RTT*rttNorm +
		w.Loss*lossScore +
		w.Stability*stab -
		w.Load*load
}

// BestPeer picks the peer address from candidates with the highest Score.
// Returns "" if candidates is empty.
// When two peers have equal scores, the first in the slice wins.
func BestPeer(candidates []string, m *monitor.Map, w Weights) string {
	if len(candidates) == 0 {
		return ""
	}
	if len(candidates) == 1 {
		return candidates[0]
	}

	best := ""
	bestScore := math.Inf(-1)

	for _, addr := range candidates {
		stats := m.Get(addr)
		s := Score(stats, w)
		if s > bestScore {
			bestScore = s
			best = addr
		}
	}
	return best
}

// ShouldYield returns true when peerAddr is NOT the best peer for pieceIdx
// and a better peer exists. Workers use this to skip pieces that another
// peer would handle more efficiently, without blocking — if the preferred
// peer is also busy, the caller falls back to any available claimable piece.
func ShouldYield(peerAddr string, candidates []string, m *monitor.Map, w Weights) bool {
	if len(candidates) <= 1 {
		return false // only one option — no point yielding
	}
	best := BestPeer(candidates, m, w)
	return best != "" && best != peerAddr
}
