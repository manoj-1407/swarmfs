package peer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/manoj-1407/swarmfs/internal/piece"
	"github.com/manoj-1407/swarmfs/internal/protocol"
)

// Seeder listens for incoming peer connections and serves pieces.
// It is owned and started by Node.
type Seeder struct {
	cfg      *Config
	store    *piece.Store
	manifest *piece.Manifest
	bitfield *protocol.Bitfield
	ln       net.Listener  // written once in start(), before accept() runs
	boundAddr atomic.Value // stores string; written once in start(), read by ListenAddr
}

func newSeeder(cfg *Config, store *piece.Store, manifest *piece.Manifest, bf *protocol.Bitfield) *Seeder {
	return &Seeder{
		cfg:      cfg,
		store:    store,
		manifest: manifest,
		bitfield: bf,
	}
}

// ListenAddr returns the address the seeder is actually bound to.
// Returns "" until start() has been called. Race-free via atomic.Value.
func (s *Seeder) ListenAddr() string {
	if v := s.boundAddr.Load(); v != nil {
		return v.(string)
	}
	return ""
}

// start binds the TCP listener and publishes ListenAddr(). Must be called
// before accept(), in the same goroutine that will call accept().
func (s *Seeder) start() error {
	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("seeder listen on %s: %w", s.cfg.ListenAddr, err)
	}
	s.ln = ln
	s.boundAddr.Store(ln.Addr().String()) // publish before any other goroutine reads
	return nil
}

// accept runs the connection accept loop. Blocks until ctx is cancelled.
// Must be called after start() in the same goroutine.
func (s *Seeder) accept(ctx context.Context) error {
	ln := s.ln // safe: sequential after start() in the same goroutine
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("seeder: accept error: %v", err)
			continue
		}
		go s.handlePeer(conn)
	}
}

// handlePeer manages one leecher connection for its full lifetime.
func (s *Seeder) handlePeer(raw net.Conn) {
	defer raw.Close()
	c := protocol.NewConn(raw)

	msg, err := c.Recv()
	if err != nil {
		return
	}
	if msg.Type != protocol.TypeHandshake {
		log.Printf("seeder: expected HANDSHAKE, got %s from %s", msg.Type, raw.RemoteAddr())
		return
	}
	var hs protocol.HandshakePayload
	if err := protocol.UnmarshalPayload(msg, &hs); err != nil {
		return
	}
	if hs.FileHash != s.manifest.FileHash {
		log.Printf("seeder: file hash mismatch from %s: got %s want %s",
			raw.RemoteAddr(), hs.FileHash, s.manifest.FileHash)
		return
	}

	if err := c.Send(protocol.TypeHandshake, protocol.HandshakePayload{
		PeerID:   s.cfg.PeerID,
		FileHash: s.manifest.FileHash,
		Bitfield: s.bitfield.Encode(),
	}); err != nil {
		return
	}

	for {
		msg, err := c.Recv()
		if err != nil {
			return
		}

		switch msg.Type {
		case protocol.TypeManifestReq:
			if err := s.sendManifest(c, msg); err != nil {
				log.Printf("seeder: send manifest to %s: %v", raw.RemoteAddr(), err)
				return
			}
		case protocol.TypeRequest:
			if err := s.sendPiece(c, msg); err != nil {
				log.Printf("seeder: send piece to %s: %v", raw.RemoteAddr(), err)
				return
			}
		case protocol.TypeProbe:
			// respond immediately with acked timestamp for RTT measurement
			var probe protocol.ProbePayload
			if err := protocol.UnmarshalPayload(msg, &probe); err != nil {
				return
			}
			if err := c.Send(protocol.TypeProbeAck, protocol.ProbeAckPayload{
				Seq:     probe.Seq,
				SentAt:  probe.SentAt,
				AckedAt: time.Now().UnixNano(),
			}); err != nil {
				return
			}
		case protocol.TypeNotInterested:
			return
		default:
			log.Printf("seeder: unexpected message %s from %s", msg.Type, raw.RemoteAddr())
		}
	}
}

func (s *Seeder) sendManifest(c *protocol.Conn, msg *protocol.Message) error {
	var req protocol.ManifestReqPayload
	if err := protocol.UnmarshalPayload(msg, &req); err != nil {
		return err
	}
	if req.FileHash != s.manifest.FileHash {
		return fmt.Errorf("manifest request for wrong file: %s", req.FileHash)
	}
	raw, err := json.Marshal(s.manifest)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	return c.Send(protocol.TypeManifest, protocol.ManifestPayload{
		FileHash: s.manifest.FileHash,
		Raw:      raw,
	})
}

func (s *Seeder) sendPiece(c *protocol.Conn, msg *protocol.Message) error {
	var req protocol.RequestPayload
	if err := protocol.UnmarshalPayload(msg, &req); err != nil {
		return err
	}
	if !s.bitfield.Has(req.PieceIndex) {
		return fmt.Errorf("requested piece %d but we don't have it", req.PieceIndex)
	}
	// simulate a slow/degraded peer — used in experiments to measure scheduler behaviour
	if s.cfg.SlowMS > 0 {
		time.Sleep(time.Duration(s.cfg.SlowMS) * time.Millisecond)
	}
	data, err := s.store.Read(req.PieceIndex)
	if err != nil {
		return fmt.Errorf("read piece %d: %w", req.PieceIndex, err)
	}
	return c.Send(protocol.TypePiece, protocol.PiecePayload{
		PieceIndex: req.PieceIndex,
		Data:       data,
		Hash:       piece.HashBytes(data),
	})
}
