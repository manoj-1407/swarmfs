// Package metrics provides lightweight per-download event collection
// for SwarmFS experiments. Events are recorded in memory and exported
// to CSV for analysis.
package metrics

import (
	"encoding/csv"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Event records one observable event during a download.
type Event struct {
	ElapsedMS  int64   // milliseconds since download start
	Type       string  // "piece" | "switch" | "fail" | "probe"
	PeerAddr   string
	PieceIdx   int
	Bytes      int
	DurationMS float64
	RTTMS      float64
	TputMbps   float64
}

// Collector accumulates download events in memory.
// All methods are safe for concurrent use.
type Collector struct {
	mu        sync.Mutex
	events    []Event
	startTime time.Time

	pieces  atomic.Int64
	bytes   atomic.Int64
	switches atomic.Int64
	failed  atomic.Int64
}

// NewCollector returns a Collector with its clock started now.
func NewCollector() *Collector {
	return &Collector{startTime: time.Now()}
}

// RecordPiece logs a successfully downloaded piece.
func (c *Collector) RecordPiece(peerAddr string, idx, byteCount int, dur time.Duration, rttMS, tputMbps float64) {
	c.pieces.Add(1)
	c.bytes.Add(int64(byteCount))
	c.append(Event{
		Type: "piece", PeerAddr: peerAddr, PieceIdx: idx,
		Bytes: byteCount, DurationMS: ms(dur), RTTMS: rttMS, TputMbps: tputMbps,
	})
}

// RecordSwitch logs a peer connection failure that forced a worker to exit.
func (c *Collector) RecordSwitch(fromPeer string) {
	c.switches.Add(1)
	c.append(Event{Type: "switch", PeerAddr: fromPeer})
}

// RecordFail logs a piece download failure.
func (c *Collector) RecordFail(peerAddr string, idx int) {
	c.failed.Add(1)
	c.append(Event{Type: "fail", PeerAddr: peerAddr, PieceIdx: idx})
}

// RecordProbe logs an RTT probe result.
func (c *Collector) RecordProbe(peerAddr string, rttMS float64, acked bool) {
	kind := "probe"
	if !acked {
		kind = "probe_loss"
	}
	c.append(Event{Type: kind, PeerAddr: peerAddr, RTTMS: rttMS})
}

// Summary returns aggregate counters. Call after the download finishes.
func (c *Collector) Summary() Summary {
	c.mu.Lock()
	n := len(c.events)
	c.mu.Unlock()

	elapsed := time.Since(c.startTime)
	totalBytes := c.bytes.Load()
	throughput := 0.0
	if elapsed.Seconds() > 0 {
		throughput = float64(totalBytes) / (1024 * 1024) / elapsed.Seconds()
	}

	return Summary{
		Pieces:     c.pieces.Load(),
		Bytes:      totalBytes,
		Switches:   c.switches.Load(),
		Failed:     c.failed.Load(),
		Events:     int64(n),
		Duration:   elapsed,
		TputMbps:   throughput,
	}
}

// Summary is a snapshot of collector counters.
type Summary struct {
	Pieces    int64
	Bytes     int64
	Switches  int64
	Failed    int64
	Events    int64
	Duration  time.Duration
	TputMbps  float64
}

// WriteEvents serialises the full event log to w as CSV.
func (c *Collector) WriteEvents(w io.Writer) error {
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{
		"elapsed_ms", "type", "peer", "piece_idx",
		"bytes", "duration_ms", "rtt_ms", "tput_mbps",
	}); err != nil {
		return err
	}

	c.mu.Lock()
	events := make([]Event, len(c.events))
	copy(events, c.events)
	c.mu.Unlock()

	for _, e := range events {
		if err := cw.Write([]string{
			fmt.Sprintf("%d", e.ElapsedMS),
			e.Type, e.PeerAddr,
			fmt.Sprintf("%d", e.PieceIdx),
			fmt.Sprintf("%d", e.Bytes),
			fmt.Sprintf("%.2f", e.DurationMS),
			fmt.Sprintf("%.2f", e.RTTMS),
			fmt.Sprintf("%.3f", e.TputMbps),
		}); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func (c *Collector) append(e Event) {
	e.ElapsedMS = time.Since(c.startTime).Milliseconds()
	c.mu.Lock()
	c.events = append(c.events, e)
	c.mu.Unlock()
}

func ms(d time.Duration) float64 { return float64(d.Milliseconds()) }
