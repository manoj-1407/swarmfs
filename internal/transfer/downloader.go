// Package transfer implements parallel piece downloading with optional
// network-aware peer selection.
package transfer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/manoj-1407/swarmfs/internal/monitor"
	"github.com/manoj-1407/swarmfs/internal/piece"
	"github.com/manoj-1407/swarmfs/internal/protocol"
	"github.com/manoj-1407/swarmfs/internal/scheduler"
	"github.com/manoj-1407/swarmfs/internal/swarm"
)

const (
	defaultWorkers     = 4
	workerPollInterval = 30 * time.Millisecond
	workerMaxIdle      = 5 * time.Second
)

// Config holds network parameters for a Downloader.
type Config struct {
	PeerID         string
	FileHash       string
	DialTimeout    time.Duration
	RequestTimeout time.Duration
	Workers        int  // parallel peer connections; defaults to 4
	Adaptive       bool // if true, use scored peer selection
}

func (c *Config) workers() int {
	if c.Workers <= 0 {
		return defaultWorkers
	}
	return c.Workers
}

func (c *Config) dialTimeout() time.Duration {
	if c.DialTimeout == 0 {
		return 10 * time.Second
	}
	return c.DialTimeout
}

func (c *Config) reqTimeout() time.Duration {
	if c.RequestTimeout == 0 {
		return 30 * time.Second
	}
	return c.RequestTimeout
}

// Metrics counts events during a download run.
// Safe for concurrent use.
type Metrics struct {
	PiecesOK       atomic.Int64
	FailedAttempts atomic.Int64
	PeerSwitches   atomic.Int64 // connections that died mid-transfer
	StartTime      time.Time
	EndTime        time.Time
}

// MetricsSummary is a plain copyable snapshot of Metrics.
type MetricsSummary struct {
	PiecesOK       int64
	FailedAttempts int64
	PeerSwitches   int64
	Duration       time.Duration
}

// Summary returns a copyable snapshot. Call after Run() returns.
func (m *Metrics) Summary() MetricsSummary {
	return MetricsSummary{
		PiecesOK:       m.PiecesOK.Load(),
		FailedAttempts: m.FailedAttempts.Load(),
		PeerSwitches:   m.PeerSwitches.Load(),
		Duration:       m.EndTime.Sub(m.StartTime),
	}
}

// Downloader fetches pieces from multiple peers in parallel using rarest-first
// scheduling and claim-based duplicate prevention.
type Downloader struct {
	cfg      Config
	manifest *piece.Manifest
	store    *piece.Store
	bitfield *protocol.Bitfield // shared with the calling Node
	state    *swarm.State
	statsMap *monitor.Map
	weights  scheduler.Weights
	Metrics  Metrics
	onPiece  func(int)
}

// NewDownloader returns a Downloader ready to run.
// If cfg.Adaptive is true, the downloader uses network-aware peer selection.
func NewDownloader(
	cfg Config,
	manifest *piece.Manifest,
	store *piece.Store,
	bitfield *protocol.Bitfield,
	state *swarm.State,
	onPiece func(int),
) *Downloader {
	return &Downloader{
		cfg:      cfg,
		manifest: manifest,
		store:    store,
		bitfield: bitfield,
		state:    state,
		statsMap: monitor.NewMap(),
		weights:  scheduler.Default,
		onPiece:  onPiece,
	}
}

// Run connects to all peers (up to cfg.Workers in parallel) and downloads
// all missing pieces. Returns nil when the bitfield is complete.
func (d *Downloader) Run(ctx context.Context, peers []protocol.PeerInfo) error {
	d.Metrics.StartTime = time.Now()
	defer func() { d.Metrics.EndTime = time.Now() }()

	if len(peers) == 0 {
		return fmt.Errorf("no peers provided")
	}

	for _, p := range peers {
		bf, err := protocol.DecodeBitfield(p.Bitfield, d.manifest.PieceCount)
		if err != nil {
			log.Printf("downloader: decode bitfield for %s: %v (skipping)", p.Addr, err)
			continue
		}
		d.state.AddPeer(p.Addr, bf)
		d.statsMap.Register(p.Addr)
	}

	limit := d.cfg.workers()
	if limit > len(peers) {
		limit = len(peers)
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, limit)

	for _, p := range peers {
		if d.bitfield.Complete() {
			break
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			goto done
		}

		wg.Add(1)
		go func(peer protocol.PeerInfo) {
			defer func() {
				d.state.RemovePeer(peer.Addr)
				d.statsMap.Remove(peer.Addr)
				<-sem
				wg.Done()
			}()
			d.workerLoop(ctx, peer.Addr)
		}(p)
	}

done:
	wg.Wait()

	if !d.bitfield.Complete() {
		missing := d.bitfield.Missing()
		return fmt.Errorf("download incomplete: %d pieces still missing: %v", len(missing), missing)
	}
	return nil
}

// workerLoop maintains one persistent TCP connection to peerAddr and downloads
// pieces using rarest-first ordering. In adaptive mode it yields pieces that
// another peer would handle more efficiently.
func (d *Downloader) workerLoop(ctx context.Context, peerAddr string) {
	conn, theirBF, err := d.connect(ctx, peerAddr)
	if err != nil {
		log.Printf("downloader: connect %s: %v", peerAddr, err)
		return
	}
	defer conn.Close()

	idleStart := time.Time{}

	for {
		if ctx.Err() != nil || d.bitfield.Complete() {
			return
		}

		assigned := d.pickPiece(peerAddr, theirBF)

		if assigned == -1 {
			if !d.state.HasAnySources(d.bitfield) {
				return
			}
			if idleStart.IsZero() {
				idleStart = time.Now()
			} else if time.Since(idleStart) > workerMaxIdle {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(workerPollInterval):
			}
			continue
		}

		idleStart = time.Time{}
		d.statsMap.IncActiveReqs(peerAddr)

		start := time.Now()
		data, err := d.fetchPiece(ctx, conn, assigned)
		elapsed := time.Since(start)

		d.statsMap.DecActiveReqs(peerAddr)

		if err != nil {
			d.state.Release(assigned)
			d.Metrics.FailedAttempts.Add(1)
			log.Printf("downloader: piece %d from %s: %v", assigned, peerAddr, err)
			d.Metrics.PeerSwitches.Add(1)
			return // connection broken — exit, other workers pick up the slack
		}

		// update stats from this real transfer
		d.statsMap.RecordTransfer(peerAddr, len(data), elapsed)

		if err := d.store.Write(assigned, data); err != nil {
			d.state.Release(assigned)
			log.Printf("downloader: store piece %d: %v", assigned, err)
			return
		}
		if err := d.bitfield.Set(assigned); err != nil {
			d.state.Release(assigned)
			return
		}
		d.state.Release(assigned)
		d.Metrics.PiecesOK.Add(1)

		if d.onPiece != nil {
			d.onPiece(assigned)
		}
	}
}

// pickPiece selects the next piece to claim for peerAddr.
// In adaptive mode it skips pieces where a better peer is available.
// Returns -1 if nothing is claimable right now.
func (d *Downloader) pickPiece(peerAddr string, theirBF *protocol.Bitfield) int {
	candidates := d.state.RarestMissing(d.bitfield)

	// first pass: try to find a piece this peer should handle
	for _, idx := range candidates {
		if !theirBF.Has(idx) {
			continue
		}
		if d.cfg.Adaptive {
			peers := d.state.PeersWithPiece(idx)
			if scheduler.ShouldYield(peerAddr, peers, d.statsMap, d.weights) {
				continue // let a better peer claim it
			}
		}
		if d.state.Claim(idx) {
			return idx
		}
	}

	// second pass (fallback): in adaptive mode, take anything claimable
	// if the preferred peer hasn't picked it up (avoids starvation)
	if d.cfg.Adaptive {
		for _, idx := range candidates {
			if !theirBF.Has(idx) {
				continue
			}
			if d.state.Claim(idx) {
				return idx
			}
		}
	}

	return -1
}

// connect dials peerAddr and performs the SwarmFS handshake.
func (d *Downloader) connect(ctx context.Context, peerAddr string) (*protocol.Conn, *protocol.Bitfield, error) {
	dialer := net.Dialer{Timeout: d.cfg.dialTimeout()}
	raw, err := dialer.DialContext(ctx, "tcp", peerAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("dial: %w", err)
	}

	c := protocol.NewConn(raw)
	if err := c.Send(protocol.TypeHandshake, protocol.HandshakePayload{
		PeerID:   d.cfg.PeerID,
		FileHash: d.cfg.FileHash,
		Bitfield: d.bitfield.Encode(),
	}); err != nil {
		raw.Close()
		return nil, nil, fmt.Errorf("send handshake: %w", err)
	}

	msg, err := c.Recv()
	if err != nil {
		raw.Close()
		return nil, nil, fmt.Errorf("recv handshake: %w", err)
	}
	if msg.Type != protocol.TypeHandshake {
		raw.Close()
		return nil, nil, fmt.Errorf("expected HANDSHAKE got %s", msg.Type)
	}

	var hs protocol.HandshakePayload
	if err := protocol.UnmarshalPayload(msg, &hs); err != nil {
		raw.Close()
		return nil, nil, err
	}
	if hs.FileHash != d.cfg.FileHash {
		raw.Close()
		return nil, nil, fmt.Errorf("file hash mismatch: want %s got %s", d.cfg.FileHash, hs.FileHash)
	}

	theirBF, err := protocol.DecodeBitfield(hs.Bitfield, d.manifest.PieceCount)
	if err != nil {
		raw.Close()
		return nil, nil, fmt.Errorf("decode bitfield: %w", err)
	}

	return c, theirBF, nil
}

// fetchPiece sends REQUEST and verifies the PIECE response against manifest.
func (d *Downloader) fetchPiece(ctx context.Context, c *protocol.Conn, idx int) ([]byte, error) {
	if err := c.Send(protocol.TypeRequest, protocol.RequestPayload{PieceIndex: idx}); err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	type result struct {
		data []byte
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		msg, err := c.Recv()
		if err != nil {
			ch <- result{err: err}
			return
		}
		if msg.Type != protocol.TypePiece {
			ch <- result{err: fmt.Errorf("expected PIECE got %s", msg.Type)}
			return
		}
		var pp protocol.PiecePayload
		if err := protocol.UnmarshalPayload(msg, &pp); err != nil {
			ch <- result{err: err}
			return
		}
		if pp.PieceIndex != idx {
			ch <- result{err: fmt.Errorf("index mismatch: want %d got %d", idx, pp.PieceIndex)}
			return
		}
		if err := piece.Verify(pp.Data, d.manifest.Pieces[idx].Hash); err != nil {
			ch <- result{err: fmt.Errorf("piece %d corrupt: %w", idx, err)}
			return
		}
		ch <- result{data: pp.Data}
	}()

	deadline := time.NewTimer(d.cfg.reqTimeout())
	defer deadline.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-deadline.C:
		return nil, fmt.Errorf("piece %d timed out after %s", idx, d.cfg.reqTimeout())
	case r := <-ch:
		return r.data, r.err
	}
}

// FetchManifest connects to peerAddr, requests the manifest, and returns it.
func FetchManifest(ctx context.Context, peerID, fileHash, peerAddr string, dialTimeout time.Duration) (*piece.Manifest, error) {
	dialer := net.Dialer{Timeout: dialTimeout}
	raw, err := dialer.DialContext(ctx, "tcp", peerAddr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", peerAddr, err)
	}
	defer func() {
		c := protocol.NewConn(raw)
		_ = c.Send(protocol.TypeNotInterested, protocol.EmptyPayload)
		raw.Close()
	}()

	c := protocol.NewConn(raw)
	empty := protocol.NewBitfield(0)
	if err := c.Send(protocol.TypeHandshake, protocol.HandshakePayload{
		PeerID:   peerID,
		FileHash: fileHash,
		Bitfield: empty.Encode(),
	}); err != nil {
		return nil, fmt.Errorf("send handshake: %w", err)
	}
	if _, err := c.Recv(); err != nil {
		return nil, fmt.Errorf("recv handshake: %w", err)
	}

	if err := c.Send(protocol.TypeManifestReq, protocol.ManifestReqPayload{FileHash: fileHash}); err != nil {
		return nil, fmt.Errorf("send manifest req: %w", err)
	}

	msg, err := c.Recv()
	if err != nil {
		return nil, fmt.Errorf("recv manifest: %w", err)
	}
	if msg.Type != protocol.TypeManifest {
		return nil, fmt.Errorf("expected MANIFEST got %s", msg.Type)
	}

	var mp protocol.ManifestPayload
	if err := protocol.UnmarshalPayload(msg, &mp); err != nil {
		return nil, err
	}
	var m piece.Manifest
	if err := json.Unmarshal(mp.Raw, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	if m.FileHash != fileHash {
		return nil, fmt.Errorf("manifest hash mismatch: want %s got %s", fileHash, m.FileHash)
	}
	return &m, nil
}
