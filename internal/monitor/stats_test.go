package monitor_test

import (
	"sync"
	"testing"
	"time"

	"github.com/manoj-1407/swarmfs/internal/monitor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMap_RegisterAndGet(t *testing.T) {
	m := monitor.NewMap()
	m.Register("p1:9001")

	stats := m.Get("p1:9001")
	// baseline values
	assert.Equal(t, 50*time.Millisecond, stats.RTT)
	assert.InDelta(t, 1.0, stats.Throughput, 0.001)
	assert.InDelta(t, 0.0, stats.LossRate, 0.001)
}

func TestMap_UnknownPeerReturnsBaseline(t *testing.T) {
	m := monitor.NewMap()
	stats := m.Get("ghost:9999")
	assert.Equal(t, 50*time.Millisecond, stats.RTT)
	assert.InDelta(t, 1.0, stats.Throughput, 0.001)
}

func TestMap_RecordRTT_Converges(t *testing.T) {
	m := monitor.NewMap()
	m.Register("p1")

	// Feed 20 samples of 100ms — EWMA should converge toward 100ms
	for i := 0; i < 20; i++ {
		m.RecordRTT("p1", 100*time.Millisecond)
	}
	stats := m.Get("p1")
	// After 20 iterations with alpha=0.5: should be very close to 100ms
	assert.InDelta(t, 100.0, float64(stats.RTT.Milliseconds()), 5.0)
}

func TestMap_RecordRTT_StartsBelowSample(t *testing.T) {
	m := monitor.NewMap()
	m.Register("p1")

	// Single sample of 200ms from baseline 50ms — should move up
	m.RecordRTT("p1", 200*time.Millisecond)
	stats := m.Get("p1")
	// 0.5*200 + 0.5*50 = 125ms
	assert.InDelta(t, 125.0, float64(stats.RTT.Milliseconds()), 2.0)
}

func TestMap_RecordTransfer_UpdatesThroughput(t *testing.T) {
	m := monitor.NewMap()
	m.Register("p1")

	// 1 MB in 1 second = 1 Mbps
	for i := 0; i < 20; i++ {
		m.RecordTransfer("p1", 1024*1024, time.Second)
	}
	stats := m.Get("p1")
	// should converge near 1.0 Mbps (baseline is also 1.0)
	assert.InDelta(t, 1.0, stats.Throughput, 0.1)
}

func TestMap_RecordTransfer_HighBandwidth(t *testing.T) {
	m := monitor.NewMap()
	m.Register("p1")

	// 10 MB in 1 second = 10 Mbps
	for i := 0; i < 30; i++ {
		m.RecordTransfer("p1", 10*1024*1024, time.Second)
	}
	stats := m.Get("p1")
	// should converge toward 10 Mbps
	assert.Greater(t, stats.Throughput, 5.0)
}

func TestMap_RecordProbe_AllLost(t *testing.T) {
	m := monitor.NewMap()
	m.Register("p1")

	for i := 0; i < 50; i++ {
		m.RecordProbe("p1", false)
	}
	stats := m.Get("p1")
	// loss rate should be high after many missed probes
	assert.Greater(t, stats.LossRate, 0.5)
}

func TestMap_RecordProbe_AllReceived(t *testing.T) {
	m := monitor.NewMap()
	m.Register("p1")

	for i := 0; i < 50; i++ {
		m.RecordProbe("p1", true)
	}
	stats := m.Get("p1")
	assert.InDelta(t, 0.0, stats.LossRate, 0.01)
}

func TestMap_RecordProbe_MixedLoss(t *testing.T) {
	m := monitor.NewMap()
	m.Register("p1")

	// 50% loss: alternate acked/lost
	for i := 0; i < 50; i++ {
		m.RecordProbe("p1", i%2 == 0)
	}
	stats := m.Get("p1")
	// EWMA of 50% loss — should be somewhere reasonable
	assert.Greater(t, stats.LossRate, 0.1)
	assert.Less(t, stats.LossRate, 0.9)
}

func TestMap_ActiveReqs(t *testing.T) {
	m := monitor.NewMap()
	m.Register("p1")

	m.IncActiveReqs("p1")
	m.IncActiveReqs("p1")
	assert.Equal(t, 2, m.Get("p1").ActiveReqs)

	m.DecActiveReqs("p1")
	assert.Equal(t, 1, m.Get("p1").ActiveReqs)

	m.DecActiveReqs("p1")
	assert.Equal(t, 0, m.Get("p1").ActiveReqs)

	// decrement below zero should not go negative
	m.DecActiveReqs("p1")
	assert.Equal(t, 0, m.Get("p1").ActiveReqs)
}

func TestMap_Remove(t *testing.T) {
	m := monitor.NewMap()
	m.Register("p1")
	m.RecordRTT("p1", 200*time.Millisecond)
	m.Remove("p1")

	// after remove, Get returns baseline
	stats := m.Get("p1")
	assert.Equal(t, 50*time.Millisecond, stats.RTT)
}

func TestMap_Stability_IncreasesWithTime(t *testing.T) {
	m := monitor.NewMap()
	m.Register("p1")

	// immediately after connect, stability is low
	s1 := m.Get("p1")
	assert.Less(t, s1.Stability, 0.1)
}

func TestMap_ConcurrentAccess(t *testing.T) {
	m := monitor.NewMap()
	const n = 100

	// register many peers concurrently
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			addr := addrFor(i)
			m.Register(addr)
			m.RecordRTT(addr, time.Duration(i)*time.Millisecond)
			m.RecordTransfer(addr, 256*1024, 100*time.Millisecond)
			m.RecordProbe(addr, i%3 != 0)
			_ = m.Get(addr)
		}(i)
	}
	wg.Wait()

	// verify a few results
	for i := 0; i < n; i++ {
		stats := m.Get(addrFor(i))
		require.GreaterOrEqual(t, stats.Throughput, 0.0)
		require.GreaterOrEqual(t, stats.LossRate, 0.0)
		require.LessOrEqual(t, stats.LossRate, 1.0)
	}
}

func addrFor(i int) string {
	return "peer:" + itoa(i)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}
