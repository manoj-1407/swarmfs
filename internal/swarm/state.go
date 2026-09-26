// Package swarm tracks the local view of the P2P swarm:
// which peers exist, which pieces each peer has, and the aggregate
// availability count per piece used for rarest-first scheduling.
package swarm

import (
	"sort"
	"sync"

	"github.com/manoj-1407/swarmfs/internal/protocol"
)

// State is the local view of the swarm. All methods are safe for concurrent use.
type State struct {
	mu           sync.Mutex
	peers        map[string]*protocol.Bitfield // addr → their bitfield
	availability []int                         // per piece: how many peers have it
	inFlight     map[int]struct{}              // pieces currently being downloaded
	pieceCount   int
}

// NewState returns a State for a swarm with pieceCount pieces.
func NewState(pieceCount int) *State {
	return &State{
		peers:        make(map[string]*protocol.Bitfield),
		availability: make([]int, pieceCount),
		inFlight:     make(map[int]struct{}),
		pieceCount:   pieceCount,
	}
}

// AddPeer registers addr with its piece availability.
// If addr was already registered, its old contribution is removed first.
func (s *State) AddPeer(addr string, bf *protocol.Bitfield) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if old, ok := s.peers[addr]; ok {
		s.subtractBitfield(old)
	}
	s.peers[addr] = bf
	s.addBitfield(bf)
}

// RemovePeer removes addr from the swarm (peer disconnected or left).
func (s *State) RemovePeer(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	bf, ok := s.peers[addr]
	if !ok {
		return
	}
	s.subtractBitfield(bf)
	delete(s.peers, addr)
}

// Claim atomically marks pieceIdx as in-flight.
// Returns false if it is already in-flight (claimed by another worker).
func (s *State) Claim(pieceIdx int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.inFlight[pieceIdx]; ok {
		return false
	}
	s.inFlight[pieceIdx] = struct{}{}
	return true
}

// Release removes pieceIdx from in-flight without marking it complete.
// Use this when a download fails and the piece must be retried.
func (s *State) Release(pieceIdx int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inFlight, pieceIdx)
}

// RarestMissing returns indices of pieces missing from localBF, sorted by
// ascending availability (rarest first), excluding any currently in-flight.
// Ties broken by piece index. Returns a snapshot — safe to use after the call.
func (s *State) RarestMissing(localBF *protocol.Bitfield) []int {
	s.mu.Lock()
	defer s.mu.Unlock()

	missing := localBF.Missing() // snapshot from bitfield
	result := make([]int, 0, len(missing))
	for _, idx := range missing {
		if _, inflight := s.inFlight[idx]; !inflight {
			result = append(result, idx)
		}
	}

	avail := s.availability // capture for sort closure
	sort.Slice(result, func(i, j int) bool {
		ai := avail[result[i]]
		aj := avail[result[j]]
		if ai != aj {
			return ai < aj
		}
		return result[i] < result[j]
	})
	return result
}

// PeersWithPiece returns the addresses of all peers that have pieceIdx.
func (s *State) PeersWithPiece(pieceIdx int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []string
	for addr, bf := range s.peers {
		if bf.Has(pieceIdx) {
			out = append(out, addr)
		}
	}
	return out
}

// HasAnySources returns true if at least one peer has at least one piece
// that localBF still needs.
func (s *State) HasAnySources(localBF *protocol.Bitfield) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, idx := range localBF.Missing() {
		if idx < len(s.availability) && s.availability[idx] > 0 {
			return true
		}
	}
	return false
}

// PeerCount returns the number of currently registered peers.
func (s *State) PeerCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.peers)
}

// Availability returns how many peers currently have pieceIdx.
func (s *State) Availability(pieceIdx int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if pieceIdx < 0 || pieceIdx >= len(s.availability) {
		return 0
	}
	return s.availability[pieceIdx]
}

func (s *State) addBitfield(bf *protocol.Bitfield) {
	for i := 0; i < s.pieceCount; i++ {
		if bf.Has(i) {
			s.availability[i]++
		}
	}
}

func (s *State) subtractBitfield(bf *protocol.Bitfield) {
	for i := 0; i < s.pieceCount; i++ {
		if bf.Has(i) && s.availability[i] > 0 {
			s.availability[i]--
		}
	}
}
