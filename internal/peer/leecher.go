package peer

import (
	"context"
	"fmt"
	"log"

	"github.com/manoj-1407/swarmfs/internal/piece"
	"github.com/manoj-1407/swarmfs/internal/protocol"
	"github.com/manoj-1407/swarmfs/internal/swarm"
	"github.com/manoj-1407/swarmfs/internal/tracker"
	"github.com/manoj-1407/swarmfs/internal/transfer"
)

// Leecher downloads all pieces for a file from the swarm.
type Leecher struct {
	cfg      *Config
	fileHash string
	store    *piece.Store
	bitfield *protocol.Bitfield // shared with Node
	onPiece  func(int)
}

func newLeecher(cfg *Config, fileHash string, store *piece.Store, bf *protocol.Bitfield, onPiece func(int)) *Leecher {
	return &Leecher{cfg: cfg, fileHash: fileHash, store: store, bitfield: bf, onPiece: onPiece}
}

// Download runs the full download and returns the manifest and metrics summary.
// Steps:
//  1. Fetch peer list from tracker.
//  2. Get manifest from a seeder.
//  3. Resume from any already-verified pieces on disk.
//  4. Parallel download for remaining pieces.
func (l *Leecher) Download(ctx context.Context) (*piece.Manifest, transfer.MetricsSummary, error) {
	peers, err := tracker.GetPeers(l.cfg.TrackerAddr, l.fileHash, l.cfg.PeerID)
	if err != nil {
		return nil, transfer.MetricsSummary{}, fmt.Errorf("get peers: %w", err)
	}
	if len(peers) == 0 {
		return nil, transfer.MetricsSummary{}, fmt.Errorf("no peers available for %s", l.fileHash)
	}

	manifest, err := l.fetchManifest(ctx, peers)
	if err != nil {
		return nil, transfer.MetricsSummary{}, fmt.Errorf("fetch manifest: %w", err)
	}

	*l.bitfield = *protocol.NewBitfield(manifest.PieceCount)

	resumed := l.resumeFromStore(manifest)
	if resumed > 0 {
		log.Printf("leecher [%s]: resumed %d/%d pieces from disk", l.cfg.PeerID, resumed, manifest.PieceCount)
	}

	if l.bitfield.Complete() {
		log.Printf("leecher [%s]: already complete (resume)", l.cfg.PeerID)
		return manifest, transfer.MetricsSummary{PiecesOK: int64(manifest.PieceCount)}, nil
	}

	state := swarm.NewState(manifest.PieceCount)
	dl := transfer.NewDownloader(
		transfer.Config{
			PeerID:         l.cfg.PeerID,
			FileHash:       l.fileHash,
			DialTimeout:    l.cfg.dialTimeout(),
			RequestTimeout: l.cfg.requestTimeout(),
			Workers:        4,
			Adaptive:       l.cfg.Adaptive,
		},
		manifest,
		l.store,
		l.bitfield,
		state,
		l.onPiece,
	)

	if err := dl.Run(ctx, peers); err != nil {
		return nil, dl.Metrics.Summary(), fmt.Errorf("parallel download: %w", err)
	}

	return manifest, dl.Metrics.Summary(), nil
}

func (l *Leecher) resumeFromStore(manifest *piece.Manifest) int {
	resumed := 0
	for _, entry := range manifest.Pieces {
		if !l.store.Has(entry.Index) {
			continue
		}
		data, err := l.store.Read(entry.Index)
		if err != nil {
			log.Printf("leecher: resume read piece %d: %v", entry.Index, err)
			_ = l.store.Delete(entry.Index)
			continue
		}
		if err := piece.Verify(data, entry.Hash); err != nil {
			log.Printf("leecher: resume piece %d corrupt: %v", entry.Index, err)
			_ = l.store.Delete(entry.Index)
			continue
		}
		if err := l.bitfield.Set(entry.Index); err != nil {
			continue
		}
		resumed++
	}
	return resumed
}

func (l *Leecher) fetchManifest(ctx context.Context, peers []protocol.PeerInfo) (*piece.Manifest, error) {
	for _, p := range peers {
		m, err := transfer.FetchManifest(ctx, l.cfg.PeerID, l.fileHash, p.Addr, l.cfg.dialTimeout())
		if err != nil {
			log.Printf("leecher: manifest from %s: %v", p.Addr, err)
			continue
		}
		return m, nil
	}
	return nil, fmt.Errorf("no peer returned a valid manifest for %s", l.fileHash)
}
