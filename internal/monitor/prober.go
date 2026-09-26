package monitor

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/manoj-1407/swarmfs/internal/protocol"
)

const defaultProbeInterval = 5 * time.Second

// Prober sends periodic PROBE messages over a shared connection and updates
// the stats Map with RTT and loss measurements.
//
// The connection is shared with the downloader worker — the prober ONLY sends
// PROBE and reads PROBE_ACK; the data path (REQUEST/PIECE) is handled by the
// worker separately. The seeder dispatches each message type independently, so
// interleaving works correctly.
//
// Usage: call Run(ctx) in a goroutine alongside the worker goroutine.
// Cancel ctx to stop.
type Prober struct {
	peerAddr string
	conn     *protocol.Conn
	stats    *Map
	interval time.Duration
	seq      atomic.Uint64
}

// NewProber returns a Prober that will write probes to conn and update m.
func NewProber(peerAddr string, conn *protocol.Conn, m *Map) *Prober {
	return &Prober{
		peerAddr: peerAddr,
		conn:     conn,
		stats:    m,
		interval: defaultProbeInterval,
	}
}

// NewProberWithInterval creates a Prober with a custom probe interval.
// Useful for tests.
func NewProberWithInterval(peerAddr string, conn *protocol.Conn, m *Map, interval time.Duration) *Prober {
	p := NewProber(peerAddr, conn, m)
	p.interval = interval
	return p
}

// RunPassive is a lighter alternative: instead of sending probes, it just
// receives PROBE_ACK messages that appear on the connection as a result of
// probes sent elsewhere. Not used in the current design but kept for reference.

// Run sends probes every p.interval and reads their ACKs.
// It returns when ctx is cancelled or the connection is broken.
// Probe loss (no ACK within 2×interval) is recorded in the stats map.
func (p *Prober) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	pending := make(map[uint64]time.Time) // seq → sentAt
	ackCh := make(chan *protocol.ProbeAckPayload, 16)

	// reader goroutine — reads PROBE_ACK messages off the shared connection
	// Note: the downloader worker must NOT be reading from this connection
	// simultaneously, or messages will be lost. The prober is designed to
	// run on a SEPARATE probe-only connection (see Downloader.startProber).
	go func() {
		for {
			msg, err := p.conn.Recv()
			if err != nil {
				return // connection closed
			}
			if msg.Type != protocol.TypeProbeAck {
				log.Printf("prober: unexpected message %s from %s", msg.Type, p.peerAddr)
				continue
			}
			var ack protocol.ProbeAckPayload
			if err := protocol.UnmarshalPayload(msg, &ack); err != nil {
				continue
			}
			select {
			case ackCh <- &ack:
			default: // drop if buffer full
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			seq := p.seq.Add(1)
			sentAt := time.Now()
			err := p.conn.Send(protocol.TypeProbe, protocol.ProbePayload{
				Seq:    seq,
				SentAt: sentAt.UnixNano(),
			})
			if err != nil {
				return // connection dead
			}
			pending[seq] = sentAt

			// expire probes older than 2 intervals (no ACK = loss)
			cutoff := time.Now().Add(-2 * p.interval)
			for s, t := range pending {
				if t.Before(cutoff) {
					p.stats.RecordProbe(p.peerAddr, false) // lost
					delete(pending, s)
				}
			}

		case ack := <-ackCh:
			sentAt, ok := pending[ack.Seq]
			if !ok {
				continue // stale or duplicate
			}
			delete(pending, ack.Seq)
			rtt := time.Since(sentAt)
			p.stats.RecordRTT(p.peerAddr, rtt)
			p.stats.RecordProbe(p.peerAddr, true) // received
		}
	}
}
