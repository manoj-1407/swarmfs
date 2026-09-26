package tracker_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/manoj-1407/swarmfs/internal/tracker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- registry ---

func TestRegistry_RegisterAndPeers(t *testing.T) {
	r := tracker.NewRegistry()

	r.Register("filehash1", "10.0.0.1:9001", "ff")
	r.Register("filehash1", "10.0.0.2:9002", "f0")
	r.Register("filehash2", "10.0.0.3:9003", "00")

	peers := r.Peers("filehash1", "")
	require.Len(t, peers, 2)

	addrs := map[string]bool{}
	for _, p := range peers {
		addrs[p.Addr] = true
	}
	assert.True(t, addrs["10.0.0.1:9001"])
	assert.True(t, addrs["10.0.0.2:9002"])
}

func TestRegistry_ExcludesRequester(t *testing.T) {
	r := tracker.NewRegistry()
	r.Register("hash", "peer1:9001", "ff")
	r.Register("hash", "peer2:9002", "ff")

	peers := r.Peers("hash", "peer1:9001")
	require.Len(t, peers, 1)
	assert.Equal(t, "peer2:9002", peers[0].Addr)
}

func TestRegistry_PeersUnknownFile(t *testing.T) {
	r := tracker.NewRegistry()
	peers := r.Peers("nonexistent", "")
	assert.Nil(t, peers)
}

func TestRegistry_Remove(t *testing.T) {
	r := tracker.NewRegistry()
	r.Register("hash", "peer1:9001", "ff")
	r.Register("hash", "peer2:9002", "ff")

	r.Remove("hash", "peer1:9001")

	peers := r.Peers("hash", "")
	require.Len(t, peers, 1)
	assert.Equal(t, "peer2:9002", peers[0].Addr)
}

func TestRegistry_RemoveLastPeerCleansSwarm(t *testing.T) {
	r := tracker.NewRegistry()
	r.Register("hash", "peer1:9001", "ff")
	r.Remove("hash", "peer1:9001")

	assert.Equal(t, 0, r.SwarmSize("hash"))
	assert.Nil(t, r.Peers("hash", ""))
}

func TestRegistry_UpdateHave_Idempotent(t *testing.T) {
	r := tracker.NewRegistry()
	r.Register("hash", "peer1:9001", "ff")
	// UpdateHave on known peer should not panic
	r.UpdateHave("hash", "peer1:9001")
	// UpdateHave on unknown peer should not panic
	r.UpdateHave("hash", "ghost:9999")
	r.UpdateHave("nonexistent", "ghost:9999")
}

func TestRegistry_SwarmSize(t *testing.T) {
	r := tracker.NewRegistry()
	assert.Equal(t, 0, r.SwarmSize("hash"))

	r.Register("hash", "p1:1", "ff")
	assert.Equal(t, 1, r.SwarmSize("hash"))

	r.Register("hash", "p2:2", "ff")
	assert.Equal(t, 2, r.SwarmSize("hash"))

	r.Remove("hash", "p1:1")
	assert.Equal(t, 1, r.SwarmSize("hash"))
}

func TestRegistry_OverwriteRegistration(t *testing.T) {
	r := tracker.NewRegistry()
	r.Register("hash", "peer:9001", "00")
	r.Register("hash", "peer:9001", "ff") // same peer, updated bitfield

	peers := r.Peers("hash", "")
	require.Len(t, peers, 1)
	assert.Equal(t, "ff", peers[0].Bitfield)
}

// --- server ---

func startServer(t *testing.T) (string, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	srv := tracker.NewServer("127.0.0.1:0")

	ready := make(chan string, 1)
	go func() {
		// we need the listen addr before Serve blocks
		// use a goroutine that polls
		go func() {
			for {
				addr := srv.ListenAddr()
				if addr != "" {
					ready <- addr
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()
		_ = srv.Serve(ctx)
	}()

	select {
	case addr := <-ready:
		t.Cleanup(cancel)
		return addr, cancel
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("tracker server did not start in time")
		return "", nil
	}
}

func TestServer_RegisterAndGetPeers(t *testing.T) {
	addr, _ := startServer(t)

	err := tracker.Register(addr, "filehash1", "127.0.0.1:8001", "ff")
	require.NoError(t, err)

	err = tracker.Register(addr, "filehash1", "127.0.0.1:8002", "f0")
	require.NoError(t, err)

	peers, err := tracker.GetPeers(addr, "filehash1", "")
	require.NoError(t, err)
	require.Len(t, peers, 2)
}

func TestServer_GetPeers_ExcludesRequester(t *testing.T) {
	addr, _ := startServer(t)

	require.NoError(t, tracker.Register(addr, "hash", "peer1:9001", "ff"))
	require.NoError(t, tracker.Register(addr, "hash", "peer2:9002", "ff"))

	peers, err := tracker.GetPeers(addr, "hash", "peer1:9001")
	require.NoError(t, err)
	require.Len(t, peers, 1)
	assert.Equal(t, "peer2:9002", peers[0].Addr)
}

func TestServer_GetPeers_EmptySwarm(t *testing.T) {
	addr, _ := startServer(t)

	peers, err := tracker.GetPeers(addr, "nonexistent", "")
	require.NoError(t, err)
	assert.Empty(t, peers)
}

func TestServer_AnnounceHave(t *testing.T) {
	addr, _ := startServer(t)

	require.NoError(t, tracker.Register(addr, "hash", "peer:9001", "00"))
	require.NoError(t, tracker.AnnounceHave(addr, "hash", "peer:9001", 3))
	// just verify it doesn't error — tracker doesn't parse individual bits
}

func TestServer_Leave(t *testing.T) {
	addr, _ := startServer(t)

	require.NoError(t, tracker.Register(addr, "hash", "peer1:9001", "ff"))
	require.NoError(t, tracker.Register(addr, "hash", "peer2:9002", "ff"))

	require.NoError(t, tracker.Leave(addr, "hash", "peer1:9001"))

	peers, err := tracker.GetPeers(addr, "hash", "")
	require.NoError(t, err)
	require.Len(t, peers, 1)
	assert.Equal(t, "peer2:9002", peers[0].Addr)
}

func TestServer_ConcurrentRegistrations(t *testing.T) {
	addr, _ := startServer(t)

	const n = 20
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			errs <- tracker.Register(addr, "hash",
				"127.0.0.1:"+itoa(9000+i), "ff")
		}(i)
	}

	for i := 0; i < n; i++ {
		require.NoError(t, <-errs)
	}

	peers, err := tracker.GetPeers(addr, "hash", "")
	require.NoError(t, err)
	assert.Len(t, peers, n)
}

func itoa(n int) string {
	return fmt.Sprint(n)
}
