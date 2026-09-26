package peer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"path/filepath"
	"sync"

	"github.com/manoj-1407/swarmfs/internal/piece"
	"github.com/manoj-1407/swarmfs/internal/protocol"
	"github.com/manoj-1407/swarmfs/internal/tracker"
	"github.com/manoj-1407/swarmfs/internal/transfer"
)

// Node is a SwarmFS peer. It seeds what it has and leeches what it needs.
type Node struct {
	cfg      Config
	fileHash string
	store    *piece.Store
	manifest *piece.Manifest

	mu       sync.RWMutex
	bitfield *protocol.Bitfield

	seeder  *Seeder
	leecher *Leecher
}

// NewSeedNode chunks srcFile and prepares to serve it.
func NewSeedNode(cfg Config, srcFile string) (*Node, error) {
	if cfg.PeerID == "" {
		cfg.PeerID = randomID()
	}

	store, err := piece.NewStore(filepath.Join(cfg.DataDir, "staging"))
	if err != nil {
		return nil, fmt.Errorf("new seed node: %w", err)
	}

	manifest, err := piece.Chunk(srcFile, store, piece.DefaultPieceSize)
	if err != nil {
		return nil, fmt.Errorf("new seed node chunk: %w", err)
	}

	bf := protocol.NewBitfield(manifest.PieceCount)
	for i := 0; i < manifest.PieceCount; i++ {
		if err := bf.Set(i); err != nil {
			return nil, err
		}
	}

	n := &Node{cfg: cfg, fileHash: manifest.FileHash, store: store, manifest: manifest, bitfield: bf}
	n.seeder = newSeeder(&n.cfg, store, manifest, bf)
	return n, nil
}

// NewLeechNode prepares to download fileHash from the swarm.
func NewLeechNode(cfg Config, fileHash string) (*Node, error) {
	if cfg.PeerID == "" {
		cfg.PeerID = randomID()
	}

	store, err := piece.NewStore(filepath.Join(cfg.DataDir, fileHash))
	if err != nil {
		return nil, fmt.Errorf("new leech node: %w", err)
	}

	bf := protocol.NewBitfield(0)
	n := &Node{cfg: cfg, fileHash: fileHash, store: store, bitfield: bf}
	n.leecher = newLeecher(&n.cfg, fileHash, store, bf, n.onPieceComplete)
	return n, nil
}

// Seed registers with the tracker and blocks serving pieces until ctx is done.
func (n *Node) Seed(ctx context.Context) error {
	if err := n.seeder.start(); err != nil {
		return err
	}
	if err := n.registerWithTracker(); err != nil {
		return err
	}
	defer func() {
		if err := tracker.Leave(n.cfg.TrackerAddr, n.fileHash, n.seederAddr()); err != nil {
			log.Printf("node: leave tracker: %v", err)
		}
	}()

	log.Printf("node [%s]: seeding %s on %s", n.cfg.PeerID, n.fileHash, n.seederAddr())
	return n.seeder.accept(ctx)
}

// Leech downloads the file and assembles it to outputPath.
func (n *Node) Leech(ctx context.Context, outputPath string) error {
	_, err := n.leechInternal(ctx, outputPath)
	return err
}

// LeechWithMetrics downloads the file and returns detailed metrics.
func (n *Node) LeechWithMetrics(ctx context.Context, outputPath string) (transfer.MetricsSummary, error) {
	return n.leechInternal(ctx, outputPath)
}

func (n *Node) leechInternal(ctx context.Context, outputPath string) (transfer.MetricsSummary, error) {
	if n.leecher == nil {
		return transfer.MetricsSummary{}, fmt.Errorf("node is not configured as a leecher")
	}

	manifest, metrics, err := n.leecher.Download(ctx)
	if err != nil {
		return metrics, err
	}

	n.mu.Lock()
	n.manifest = manifest
	n.mu.Unlock()

	if err := piece.Assemble(outputPath, manifest, n.store); err != nil {
		return metrics, fmt.Errorf("assemble: %w", err)
	}

	log.Printf("node [%s]: download complete → %s (pieces=%d failed=%d switches=%d)",
		n.cfg.PeerID, outputPath, metrics.PiecesOK, metrics.FailedAttempts, metrics.PeerSwitches)
	return metrics, nil
}

// ListenAddr returns the address the seeder is bound to after Seed() starts.
func (n *Node) ListenAddr() string {
	if n.seeder == nil {
		return ""
	}
	return n.seeder.ListenAddr()
}

// FileHash returns the file hash this node works with.
func (n *Node) FileHash() string { return n.fileHash }

// Bitfield returns a copy of the current piece availability bitfield.
func (n *Node) Bitfield() *protocol.Bitfield {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.bitfield
}

func (n *Node) onPieceComplete(index int) {
	if err := tracker.AnnounceHave(n.cfg.TrackerAddr, n.fileHash, n.cfg.ListenAddr, index); err != nil {
		log.Printf("node: announce have piece %d: %v", index, err)
	}
}

func (n *Node) registerWithTracker() error {
	n.mu.RLock()
	bf := n.bitfield.Encode()
	n.mu.RUnlock()
	return tracker.Register(n.cfg.TrackerAddr, n.fileHash, n.seederAddr(), bf)
}

func (n *Node) seederAddr() string {
	if n.seeder != nil {
		if addr := n.seeder.ListenAddr(); addr != "" {
			return addr
		}
	}
	addr := n.cfg.ListenAddr
	if len(addr) > 0 && addr[0] == ':' {
		return "127.0.0.1" + addr
	}
	return addr
}

func randomID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
