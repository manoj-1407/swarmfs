package swarm_test

import (
	"testing"

	"github.com/manoj-1407/swarmfs/internal/protocol"
	"github.com/manoj-1407/swarmfs/internal/swarm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func bfWith(t *testing.T, total int, set ...int) *protocol.Bitfield {
	t.Helper()
	bf := protocol.NewBitfield(total)
	for _, i := range set {
		require.NoError(t, bf.Set(i))
	}
	return bf
}

func TestState_AddPeerUpdatesAvailability(t *testing.T) {
	s := swarm.NewState(4)
	s.AddPeer("p1", bfWith(t, 4, 0, 1))
	s.AddPeer("p2", bfWith(t, 4, 1, 2))

	assert.Equal(t, 1, s.Availability(0))
	assert.Equal(t, 2, s.Availability(1))
	assert.Equal(t, 1, s.Availability(2))
	assert.Equal(t, 0, s.Availability(3))
}

func TestState_RemovePeerUpdatesAvailability(t *testing.T) {
	s := swarm.NewState(4)
	s.AddPeer("p1", bfWith(t, 4, 0, 1, 2))
	s.AddPeer("p2", bfWith(t, 4, 1, 2))

	s.RemovePeer("p1")

	assert.Equal(t, 0, s.Availability(0))
	assert.Equal(t, 1, s.Availability(1))
	assert.Equal(t, 1, s.Availability(2))
}

func TestState_RemoveNonexistentPeer(t *testing.T) {
	s := swarm.NewState(4)
	// should not panic
	s.RemovePeer("ghost")
}

func TestState_AddPeerOverwrite(t *testing.T) {
	s := swarm.NewState(4)
	s.AddPeer("p1", bfWith(t, 4, 0))
	s.AddPeer("p1", bfWith(t, 4, 1)) // same peer, updated bitfield

	assert.Equal(t, 0, s.Availability(0), "old contribution removed")
	assert.Equal(t, 1, s.Availability(1), "new contribution added")
}

func TestState_RarestMissing_Order(t *testing.T) {
	s := swarm.NewState(5)
	// piece 0: 3 peers, piece 1: 1 peer, piece 2: 2 peers, piece 3: 0 peers, piece 4: 1 peer
	s.AddPeer("p1", bfWith(t, 5, 0, 1, 2))
	s.AddPeer("p2", bfWith(t, 5, 0, 2, 4))
	s.AddPeer("p3", bfWith(t, 5, 0))

	localBF := protocol.NewBitfield(5) // have nothing

	result := s.RarestMissing(localBF)
	// piece 3 (avail=0), piece 1 (avail=1), piece 4 (avail=1), piece 2 (avail=2), piece 0 (avail=3)
	assert.Equal(t, []int{3, 1, 4, 2, 0}, result)
}

func TestState_RarestMissing_ExcludesAlreadyHave(t *testing.T) {
	s := swarm.NewState(4)
	s.AddPeer("p1", bfWith(t, 4, 0, 1, 2, 3))

	localBF := bfWith(t, 4, 0, 2) // we already have 0 and 2
	result := s.RarestMissing(localBF)

	assert.Equal(t, []int{1, 3}, result)
}

func TestState_RarestMissing_ExcludesInFlight(t *testing.T) {
	s := swarm.NewState(4)
	s.AddPeer("p1", bfWith(t, 4, 0, 1, 2, 3))

	localBF := protocol.NewBitfield(4)
	assert.True(t, s.Claim(1))
	assert.True(t, s.Claim(3))

	result := s.RarestMissing(localBF)
	assert.Equal(t, []int{0, 2}, result)
}

func TestState_Claim_PreventsDuplicate(t *testing.T) {
	s := swarm.NewState(4)
	assert.True(t, s.Claim(2))
	assert.False(t, s.Claim(2), "second claim must fail")
}

func TestState_Release_AllowsReClaim(t *testing.T) {
	s := swarm.NewState(4)
	assert.True(t, s.Claim(0))
	s.Release(0)
	assert.True(t, s.Claim(0), "after release, claim must succeed again")
}

func TestState_PeersWithPiece(t *testing.T) {
	s := swarm.NewState(4)
	s.AddPeer("p1", bfWith(t, 4, 0, 1))
	s.AddPeer("p2", bfWith(t, 4, 1, 2))
	s.AddPeer("p3", bfWith(t, 4, 2, 3))

	peers := s.PeersWithPiece(1)
	assert.Len(t, peers, 2)
	assert.ElementsMatch(t, []string{"p1", "p2"}, peers)

	peers = s.PeersWithPiece(3)
	assert.Len(t, peers, 1)
	assert.Equal(t, "p3", peers[0])

	peers = s.PeersWithPiece(0)
	assert.Len(t, peers, 1)
}

func TestState_HasAnySources(t *testing.T) {
	s := swarm.NewState(4)
	s.AddPeer("p1", bfWith(t, 4, 0, 1))

	localBF := protocol.NewBitfield(4)
	assert.True(t, s.HasAnySources(localBF))

	// after we have everything p1 has
	require.NoError(t, localBF.Set(0))
	require.NoError(t, localBF.Set(1))
	assert.False(t, s.HasAnySources(localBF), "no sources for remaining pieces 2,3")
}

func TestState_PeerCount(t *testing.T) {
	s := swarm.NewState(4)
	assert.Equal(t, 0, s.PeerCount())
	s.AddPeer("p1", protocol.NewBitfield(4))
	assert.Equal(t, 1, s.PeerCount())
	s.AddPeer("p2", protocol.NewBitfield(4))
	assert.Equal(t, 2, s.PeerCount())
	s.RemovePeer("p1")
	assert.Equal(t, 1, s.PeerCount())
}

func TestState_RarestMissing_AllComplete(t *testing.T) {
	s := swarm.NewState(3)
	s.AddPeer("p1", bfWith(t, 3, 0, 1, 2))
	localBF := bfWith(t, 3, 0, 1, 2)

	result := s.RarestMissing(localBF)
	assert.Empty(t, result)
}
