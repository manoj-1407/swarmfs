package scheduler_test

import (
	"testing"
	"time"

	"github.com/manoj-1407/swarmfs/internal/monitor"
	"github.com/manoj-1407/swarmfs/internal/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stats(rttMs float64, tputMbps float64, loss float64, stability float64, activeReqs int) monitor.NetworkStats {
	return monitor.NetworkStats{
		RTT:        time.Duration(rttMs * float64(time.Millisecond)),
		Throughput: tputMbps,
		LossRate:   loss,
		Stability:  stability,
		ActiveReqs: activeReqs,
	}
}

// TestScore_PerfectPeer: zero latency, max throughput, no loss, fully stable, idle.
func TestScore_PerfectPeer(t *testing.T) {
	s := stats(0, 10.0, 0.0, 1.0, 0)
	score := scheduler.Score(s, scheduler.Default)
	// should be close to 1.0 (w.Throughput + w.RTT + w.Loss + w.Stability = 0.95)
	assert.InDelta(t, 0.95, score, 0.05)
}

// TestScore_WorstPeer: sky-high latency, zero throughput, full loss.
func TestScore_WorstPeer(t *testing.T) {
	s := stats(10000, 0.0, 1.0, 0.0, 4)
	score := scheduler.Score(s, scheduler.Default)
	assert.Less(t, score, 0.1)
}

// TestScore_HighLatencyLowersThanLow.
func TestScore_HighLatencyLowersThanLow(t *testing.T) {
	low := scheduler.Score(stats(10, 5.0, 0.0, 1.0, 0), scheduler.Default)
	high := scheduler.Score(stats(300, 5.0, 0.0, 1.0, 0), scheduler.Default)
	assert.Greater(t, low, high, "lower RTT must score higher")
}

func TestScore_HighThroughputBetter(t *testing.T) {
	slow := scheduler.Score(stats(50, 1.0, 0.0, 1.0, 0), scheduler.Default)
	fast := scheduler.Score(stats(50, 8.0, 0.0, 1.0, 0), scheduler.Default)
	assert.Greater(t, fast, slow)
}

func TestScore_LossyPeerLower(t *testing.T) {
	clean := scheduler.Score(stats(50, 5.0, 0.00, 1.0, 0), scheduler.Default)
	lossy := scheduler.Score(stats(50, 5.0, 0.15, 1.0, 0), scheduler.Default)
	assert.Greater(t, clean, lossy)
}

func TestScore_LoadPenalty(t *testing.T) {
	idle := scheduler.Score(stats(50, 5.0, 0.0, 1.0, 0), scheduler.Default)
	busy := scheduler.Score(stats(50, 5.0, 0.0, 1.0, 4), scheduler.Default)
	assert.Greater(t, idle, busy)
}

func TestScore_ThroughputCappedAt1(t *testing.T) {
	// 100 Mbps — way above baseline — should not blow up the score
	s1 := scheduler.Score(stats(50, 10.0, 0.0, 1.0, 0), scheduler.Default)
	s2 := scheduler.Score(stats(50, 100.0, 0.0, 1.0, 0), scheduler.Default)
	assert.InDelta(t, s1, s2, 0.001, "throughput above baseline should be capped")
}

func TestScore_AllZeroWeights(t *testing.T) {
	w := scheduler.Weights{}
	s := scheduler.Score(stats(50, 5.0, 0.1, 0.8, 1), w)
	assert.InDelta(t, 0.0, s, 0.001)
}

func TestBestPeer_Empty(t *testing.T) {
	m := monitor.NewMap()
	best := scheduler.BestPeer(nil, m, scheduler.Default)
	assert.Equal(t, "", best)
}

func TestBestPeer_SingleCandidate(t *testing.T) {
	m := monitor.NewMap()
	m.Register("p1")
	best := scheduler.BestPeer([]string{"p1"}, m, scheduler.Default)
	assert.Equal(t, "p1", best)
}

func TestBestPeer_PicksHigherScore(t *testing.T) {
	m := monitor.NewMap()
	m.Register("slow")
	m.Register("fast")

	// make "fast" look better: record high-throughput transfers
	for i := 0; i < 20; i++ {
		m.RecordTransfer("fast", 10*1024*1024, 100*time.Millisecond) // 100 Mbps
		m.RecordRTT("fast", 5*time.Millisecond)
		m.RecordTransfer("slow", 256*1024, time.Second) // 0.25 Mbps
		m.RecordRTT("slow", 300*time.Millisecond)
	}

	best := scheduler.BestPeer([]string{"slow", "fast"}, m, scheduler.Default)
	assert.Equal(t, "fast", best)
}

func TestBestPeer_LossyPeerAvoided(t *testing.T) {
	m := monitor.NewMap()
	m.Register("clean")
	m.Register("lossy")

	for i := 0; i < 50; i++ {
		m.RecordProbe("lossy", false) // 100% loss
		m.RecordProbe("clean", true)  // 0% loss
	}

	best := scheduler.BestPeer([]string{"clean", "lossy"}, m, scheduler.Default)
	assert.Equal(t, "clean", best)
}

func TestShouldYield_SinglePeer(t *testing.T) {
	m := monitor.NewMap()
	m.Register("p1")
	assert.False(t, scheduler.ShouldYield("p1", []string{"p1"}, m, scheduler.Default))
}

func TestShouldYield_BestPeerDoesNotYield(t *testing.T) {
	m := monitor.NewMap()
	m.Register("p1")
	m.Register("p2")

	for i := 0; i < 20; i++ {
		m.RecordRTT("p1", 5*time.Millisecond)
		m.RecordRTT("p2", 500*time.Millisecond)
		m.RecordTransfer("p1", 10*1024*1024, 100*time.Millisecond)
	}

	candidates := []string{"p1", "p2"}
	// p1 is fast — should NOT yield
	assert.False(t, scheduler.ShouldYield("p1", candidates, m, scheduler.Default))
	// p2 is slow — should yield
	assert.True(t, scheduler.ShouldYield("p2", candidates, m, scheduler.Default))
}

// TestScore_Ordering: verify that the ordering across many variations is sane.
// Scores are computed analytically; this test catches regressions in the formula.
//
// With Default weights (tput=0.35, rtt=0.30, loss=0.25, stab=0.05, load=0.05):
//
//	ideal        ≈ 0.923  (5ms, 10Mbps, 0% loss, full stability, idle)
//	good         ≈ 0.774  (20ms, 8Mbps, 1% loss)
//	lossy        ≈ 0.590  (50ms, 5Mbps, 10% loss) — good RTT saves it vs slow_latency
//	average      ≈ 0.543  (80ms, 5Mbps, 3% loss, 2 active reqs)
//	slow_latency ≈ 0.525  (200ms, 5Mbps, 0% loss) — 200ms RTT costs more than 10% loss
//	bad          ≈ 0.255  (300ms, 1Mbps, 20% loss)
//	worst        ≈ 0.091  (2000ms, 0.1Mbps, 50% loss)
func TestScore_Ordering(t *testing.T) {
	tests := []struct {
		name  string
		stats monitor.NetworkStats
	}{
		{"ideal", stats(5, 10, 0.00, 1.0, 0)},
		{"good", stats(20, 8, 0.01, 0.9, 1)},
		{"lossy", stats(50, 5, 0.10, 0.8, 0)},
		{"average", stats(80, 5, 0.03, 0.7, 2)},
		{"slow_latency", stats(200, 5, 0.00, 0.8, 0)},
		{"bad", stats(300, 1, 0.20, 0.3, 3)},
		{"worst", stats(2000, 0.1, 0.50, 0.1, 4)},
	}

	scores := make([]float64, len(tests))
	for i, tt := range tests {
		scores[i] = scheduler.Score(tt.stats, scheduler.Default)
	}

	for i := 1; i < len(scores); i++ {
		require.GreaterOrEqual(t, scores[i-1], scores[i],
			"expected %s (%.4f) ≥ %s (%.4f)",
			tests[i-1].name, scores[i-1], tests[i].name, scores[i])
	}
}
