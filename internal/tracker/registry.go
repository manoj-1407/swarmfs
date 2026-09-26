// Package tracker implements the SwarmFS discovery and registration service.
package tracker

import (
	"sync"
	"time"

	"github.com/manoj-1407/swarmfs/internal/protocol"
)

// peerRecord holds what the tracker knows about one peer in one swarm.
type peerRecord struct {
	addr     string
	bitfield string // raw hex, opaque to the tracker
	lastSeen time.Time
}

// Registry is the tracker's in-memory swarm table.
// All methods are safe for concurrent use.
type Registry struct {
	mu     sync.RWMutex
	swarms map[string]map[string]*peerRecord // fileHash → addr → record
}

// NewRegistry returns a ready Registry.
func NewRegistry() *Registry {
	return &Registry{
		swarms: make(map[string]map[string]*peerRecord),
	}
}

// Register adds or updates a peer's record for a given file.
func (r *Registry) Register(fileHash, peerAddr, bitfield string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.swarms[fileHash] == nil {
		r.swarms[fileHash] = make(map[string]*peerRecord)
	}
	r.swarms[fileHash][peerAddr] = &peerRecord{
		addr:     peerAddr,
		bitfield: bitfield,
		lastSeen: time.Now(),
	}
}

// UpdateHave marks that peerAddr now has piece pieceIndex for fileHash.
// The tracker stores bitfields as opaque hex, so it can't update individual bits —
// the peer must re-register with a fresh bitfield on reconnect.
// For HAVE messages, we just refresh lastSeen.
func (r *Registry) UpdateHave(fileHash, peerAddr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	swarm := r.swarms[fileHash]
	if swarm == nil {
		return
	}
	rec, ok := swarm[peerAddr]
	if !ok {
		return
	}
	rec.lastSeen = time.Now()
}

// Peers returns all peer records for fileHash, excluding excludeAddr.
// Returns nil if no swarm exists for fileHash.
func (r *Registry) Peers(fileHash, excludeAddr string) []protocol.PeerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	swarm := r.swarms[fileHash]
	if len(swarm) == 0 {
		return nil
	}

	out := make([]protocol.PeerInfo, 0, len(swarm))
	for addr, rec := range swarm {
		if addr == excludeAddr {
			continue
		}
		out = append(out, protocol.PeerInfo{
			Addr:     rec.addr,
			Bitfield: rec.bitfield,
		})
	}
	return out
}

// Remove deletes a peer from all swarms it was registered in.
func (r *Registry) Remove(fileHash, peerAddr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if swarm := r.swarms[fileHash]; swarm != nil {
		delete(swarm, peerAddr)
		if len(swarm) == 0 {
			delete(r.swarms, fileHash)
		}
	}
}

// SwarmSize returns the number of peers registered for fileHash.
func (r *Registry) SwarmSize(fileHash string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.swarms[fileHash])
}
