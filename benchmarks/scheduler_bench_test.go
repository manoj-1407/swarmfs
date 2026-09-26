package benchmarks

import (
	"fmt"
	"testing"
	"time"

	"github.com/manoj-1407/swarmfs/internal/monitor"
	"github.com/manoj-1407/swarmfs/internal/protocol"
	"github.com/manoj-1407/swarmfs/internal/scheduler"
	"github.com/manoj-1407/swarmfs/internal/swarm"
)

// representative stats for a mid-quality peer
var benchStats = monitor.NetworkStats{
	RTT:        80 * time.Millisecond,
	Throughput: 5.0,
	LossRate:   0.02,
	Stability:  0.9,
	ActiveReqs: 1,
}

// BenchmarkScore measures the cost of a single peer scoring call.
// This runs inside the hot path of workerLoop — must stay cheap.
func BenchmarkScore(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = scheduler.Score(benchStats, scheduler.Default)
	}
}

// BenchmarkBestPeer_5Peers simulates a typical small swarm.
func BenchmarkBestPeer_5Peers(b *testing.B) {
	m := buildMap(5)
	peers := peerList(5)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = scheduler.BestPeer(peers, m, scheduler.Default)
	}
}

// BenchmarkBestPeer_50Peers simulates a larger swarm.
func BenchmarkBestPeer_50Peers(b *testing.B) {
	m := buildMap(50)
	peers := peerList(50)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = scheduler.BestPeer(peers, m, scheduler.Default)
	}
}

// BenchmarkBestPeer_100Peers stress-tests the selector.
func BenchmarkBestPeer_100Peers(b *testing.B) {
	m := buildMap(100)
	peers := peerList(100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = scheduler.BestPeer(peers, m, scheduler.Default)
	}
}

// BenchmarkRarestMissing_4096Pieces measures swarm state scheduling with
// a realistic large piece count (1 GB file at 256 KB pieces).
func BenchmarkRarestMissing_4096Pieces(b *testing.B) {
	const pieces = 4096
	const peers = 5

	s := swarm.NewState(pieces)
	for i := 0; i < peers; i++ {
		bf := protocol.NewBitfield(pieces)
		// each peer has ~80% of pieces
		for j := 0; j < pieces; j++ {
			if (j+i)%5 != 0 {
				_ = bf.Set(j)
			}
		}
		s.AddPeer(fmt.Sprintf("peer%d:900%d", i, i), bf)
	}

	localBF := protocol.NewBitfield(pieces)
	// we have 10% already
	for j := 0; j < pieces/10; j++ {
		_ = localBF.Set(j)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = s.RarestMissing(localBF)
	}
}

// BenchmarkRarestMissing_256Pieces is the common case for 64 MB files.
func BenchmarkRarestMissing_256Pieces(b *testing.B) {
	const pieces = 256
	const peers = 4

	s := swarm.NewState(pieces)
	for i := 0; i < peers; i++ {
		bf := protocol.NewBitfield(pieces)
		for j := 0; j < pieces; j++ {
			if j%3 != i%3 {
				_ = bf.Set(j)
			}
		}
		s.AddPeer(fmt.Sprintf("peer%d:900%d", i, i), bf)
	}

	localBF := protocol.NewBitfield(pieces)
	for j := 0; j < pieces/4; j++ {
		_ = localBF.Set(j)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = s.RarestMissing(localBF)
	}
}

// BenchmarkShouldYield measures the adaptive yield decision in the hot path.
func BenchmarkShouldYield_10Peers(b *testing.B) {
	m := buildMap(10)
	peers := peerList(10)
	self := peers[0]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = scheduler.ShouldYield(self, peers, m, scheduler.Default)
	}
}

// --- helpers ---

func buildMap(n int) *monitor.Map {
	m := monitor.NewMap()
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("peer%d:9000", i)
		m.Register(addr)
		// vary stats so BestPeer has meaningful work to do
		m.RecordRTT(addr, time.Duration(10+i*7)*time.Millisecond)
		m.RecordTransfer(addr, 256*1024,
			time.Duration(100+i*50)*time.Millisecond)
		if i%4 == 0 {
			for j := 0; j < 5; j++ {
				m.RecordProbe(addr, false) // some loss
			}
		}
	}
	return m
}

func peerList(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("peer%d:9000", i)
	}
	return out
}
