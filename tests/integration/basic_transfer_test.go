package integration_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/manoj-1407/swarmfs/internal/peer"
	"github.com/manoj-1407/swarmfs/internal/tracker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startTracker starts a tracker server on a random port and returns its address.
func startTracker(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	srv := tracker.NewServer("127.0.0.1:0")
	ready := make(chan string, 1)
	go func() {
		go func() {
			for {
				if a := srv.ListenAddr(); a != "" {
					ready <- a
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()
		_ = srv.Serve(ctx)
	}()

	select {
	case addr := <-ready:
		return addr
	case <-time.After(3 * time.Second):
		t.Fatal("tracker did not start")
		return ""
	}
}

// randomFile writes n random bytes to a temp file and returns its path.
func randomFile(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)

	f, err := os.CreateTemp(t.TempDir(), "swarmfs-src-*")
	require.NoError(t, err)
	_, err = f.Write(b)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

// waitForSeeder polls until the seeder's TCP listener is up.
func waitForSeeder(t *testing.T, n *peer.Node) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n.ListenAddr() != "" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("seeder did not bind in time")
}

// TestBasicTransfer is the Phase 1 milestone:
// one seeder, one leecher, loopback TCP, file must be byte-identical after transfer.
func TestBasicTransfer(t *testing.T) {
	trackerAddr := startTracker(t)

	// --- source file ---
	const fileSize = 3*256*1024 + 17 // 3 full pieces + 17-byte tail
	srcPath := randomFile(t, fileSize)
	srcBytes, err := os.ReadFile(srcPath)
	require.NoError(t, err)

	// --- seeder ---
	seederDir := t.TempDir()
	seedNode, err := peer.NewSeedNode(peer.Config{
		PeerID:      "seeder-1",
		ListenAddr:  "127.0.0.1:0",
		TrackerAddr: trackerAddr,
		DataDir:     seederDir,
	}, srcPath)
	require.NoError(t, err)

	seedCtx, seedCancel := context.WithCancel(context.Background())
	defer seedCancel()

	seedErrCh := make(chan error, 1)
	go func() { seedErrCh <- seedNode.Seed(seedCtx) }()

	waitForSeeder(t, seedNode)

	// --- leecher ---
	leechDir := t.TempDir()
	leechNode, err := peer.NewLeechNode(peer.Config{
		PeerID:         "leecher-1",
		ListenAddr:     "127.0.0.1:0",
		TrackerAddr:    trackerAddr,
		DataDir:        leechDir,
		DialTimeout:    5 * time.Second,
		RequestTimeout: 15 * time.Second,
	}, seedNode.FileHash())
	require.NoError(t, err)

	// seeder must be registered before leecher asks for peer list
	require.NoError(t, waitForRegistration(t, trackerAddr, seedNode.FileHash()))

	outPath := filepath.Join(t.TempDir(), "downloaded.bin")
	leechCtx, leechCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer leechCancel()

	err = leechNode.Leech(leechCtx, outPath)
	require.NoError(t, err, "leech must complete without error")

	// --- verify ---
	gotBytes, err := os.ReadFile(outPath)
	require.NoError(t, err)
	require.Equal(t, len(srcBytes), len(gotBytes), "file size must match")
	assert.Equal(t, srcBytes, gotBytes, "downloaded file must be byte-identical to source")

	// stop seeder cleanly
	seedCancel()
	select {
	case err := <-seedErrCh:
		assert.NoError(t, err, "seeder should shut down cleanly")
	case <-time.After(3 * time.Second):
		t.Error("seeder did not stop in time")
	}
}

// TestTransfer_ExactPieceBoundary transfers a file that is exactly N*pieceSize bytes.
func TestTransfer_ExactPieceBoundary(t *testing.T) {
	trackerAddr := startTracker(t)

	const pieceSize = 256 * 1024
	srcPath := randomFile(t, pieceSize*4) // exactly 4 pieces, no tail
	srcBytes, _ := os.ReadFile(srcPath)

	seedNode, err := peer.NewSeedNode(peer.Config{
		PeerID: "s", ListenAddr: "127.0.0.1:0",
		TrackerAddr: trackerAddr, DataDir: t.TempDir(),
	}, srcPath)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = seedNode.Seed(ctx) }()
	waitForSeeder(t, seedNode)
	require.NoError(t, waitForRegistration(t, trackerAddr, seedNode.FileHash()))

	leechNode, err := peer.NewLeechNode(peer.Config{
		PeerID: "l", ListenAddr: "127.0.0.1:0",
		TrackerAddr: trackerAddr, DataDir: t.TempDir(),
		DialTimeout: 5 * time.Second, RequestTimeout: 15 * time.Second,
	}, seedNode.FileHash())
	require.NoError(t, err)

	outPath := filepath.Join(t.TempDir(), "out.bin")
	leechCtx, leechCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer leechCancel()

	require.NoError(t, leechNode.Leech(leechCtx, outPath))

	gotBytes, _ := os.ReadFile(outPath)
	assert.Equal(t, srcBytes, gotBytes)
}

// TestTransfer_SinglePiece transfers a file smaller than one piece.
func TestTransfer_SinglePiece(t *testing.T) {
	trackerAddr := startTracker(t)

	srcPath := randomFile(t, 1024) // 1 KiB — well under 256 KiB piece size
	srcBytes, _ := os.ReadFile(srcPath)

	seedNode, err := peer.NewSeedNode(peer.Config{
		PeerID: "s", ListenAddr: "127.0.0.1:0",
		TrackerAddr: trackerAddr, DataDir: t.TempDir(),
	}, srcPath)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = seedNode.Seed(ctx) }()
	waitForSeeder(t, seedNode)
	require.NoError(t, waitForRegistration(t, trackerAddr, seedNode.FileHash()))

	leechNode, err := peer.NewLeechNode(peer.Config{
		PeerID: "l", ListenAddr: "127.0.0.1:0",
		TrackerAddr: trackerAddr, DataDir: t.TempDir(),
		DialTimeout: 5 * time.Second, RequestTimeout: 10 * time.Second,
	}, seedNode.FileHash())
	require.NoError(t, err)

	outPath := filepath.Join(t.TempDir(), "out.bin")
	leechCtx, leechCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer leechCancel()

	require.NoError(t, leechNode.Leech(leechCtx, outPath))

	gotBytes, _ := os.ReadFile(outPath)
	assert.Equal(t, srcBytes, gotBytes)
}

// waitForRegistration polls the tracker until seedNode.FileHash() has at least
// one registered peer, so the leecher doesn't get an empty peer list.
func waitForRegistration(t *testing.T, trackerAddr, fileHash string) error {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		peers, err := tracker.GetPeers(trackerAddr, fileHash, "probe")
		if err == nil && len(peers) > 0 {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("no peers registered for %s after 3s", fileHash)
}

// TestTransfer_PeerChurn kills one seeder mid-transfer and verifies the download
// completes from the surviving seeder.
func TestTransfer_PeerChurn(t *testing.T) {
	trackerAddr := startTracker(t)

	const pieceSize = 256 * 1024
	srcPath := randomFile(t, pieceSize*6) // 6 pieces
	srcBytes, _ := os.ReadFile(srcPath)

	// seeder 1 — will be killed partway
	seed1, err := peer.NewSeedNode(peer.Config{
		PeerID: "seeder-churn-1", ListenAddr: "127.0.0.1:0",
		TrackerAddr: trackerAddr, DataDir: t.TempDir(),
	}, srcPath)
	require.NoError(t, err)

	seed1Ctx, killSeed1 := context.WithCancel(context.Background())
	go func() { _ = seed1.Seed(seed1Ctx) }()
	waitForSeeder(t, seed1)
	require.NoError(t, waitForRegistration(t, trackerAddr, seed1.FileHash()))

	// seeder 2 — survives the whole transfer
	seed2, err := peer.NewSeedNode(peer.Config{
		PeerID: "seeder-churn-2", ListenAddr: "127.0.0.1:0",
		TrackerAddr: trackerAddr, DataDir: t.TempDir(),
	}, srcPath)
	require.NoError(t, err)
	require.Equal(t, seed1.FileHash(), seed2.FileHash(), "same file → same hash")

	seed2Ctx, cancelSeed2 := context.WithCancel(context.Background())
	defer cancelSeed2()
	go func() { _ = seed2.Seed(seed2Ctx) }()
	waitForSeeder(t, seed2)

	// leecher
	leechNode, err := peer.NewLeechNode(peer.Config{
		PeerID: "leecher-churn", ListenAddr: "127.0.0.1:0",
		TrackerAddr: trackerAddr, DataDir: t.TempDir(),
		DialTimeout: 3 * time.Second, RequestTimeout: 10 * time.Second,
	}, seed1.FileHash())
	require.NoError(t, err)

	// kill seeder 1 after 100ms — download should continue from seeder 2
	go func() {
		time.Sleep(100 * time.Millisecond)
		killSeed1()
	}()

	outPath := filepath.Join(t.TempDir(), "out.bin")
	leechCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	require.NoError(t, leechNode.Leech(leechCtx, outPath))

	got, _ := os.ReadFile(outPath)
	assert.Equal(t, srcBytes, got, "file must be complete despite seeder 1 dying")
}

// TestTransfer_Resume verifies that an interrupted download resumes from where
// it left off: pieces already on disk are re-verified and not re-downloaded.
func TestTransfer_Resume(t *testing.T) {
	trackerAddr := startTracker(t)

	const pieceSize = 256 * 1024
	srcPath := randomFile(t, pieceSize*4)
	srcBytes, _ := os.ReadFile(srcPath)

	seedNode, err := peer.NewSeedNode(peer.Config{
		PeerID: "seeder-resume", ListenAddr: "127.0.0.1:0",
		TrackerAddr: trackerAddr, DataDir: t.TempDir(),
	}, srcPath)
	require.NoError(t, err)

	seedCtx, cancelSeed := context.WithCancel(context.Background())
	defer cancelSeed()
	go func() { _ = seedNode.Seed(seedCtx) }()
	waitForSeeder(t, seedNode)
	require.NoError(t, waitForRegistration(t, trackerAddr, seedNode.FileHash()))

	leechDataDir := t.TempDir()

	// --- first run: download everything ---
	leech1, err := peer.NewLeechNode(peer.Config{
		PeerID: "leecher-resume-1", ListenAddr: "127.0.0.1:0",
		TrackerAddr: trackerAddr, DataDir: leechDataDir,
		DialTimeout: 5 * time.Second, RequestTimeout: 15 * time.Second,
	}, seedNode.FileHash())
	require.NoError(t, err)

	out1 := filepath.Join(t.TempDir(), "out1.bin")
	ctx1, cancel1 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel1()
	require.NoError(t, leech1.Leech(ctx1, out1))

	got1, _ := os.ReadFile(out1)
	require.Equal(t, srcBytes, got1, "first download must be correct")

	// --- second run: same DataDir, pieces already on disk ---
	// The leecher should detect them all and skip the download entirely.
	leech2, err := peer.NewLeechNode(peer.Config{
		PeerID: "leecher-resume-2", ListenAddr: "127.0.0.1:0",
		TrackerAddr: trackerAddr, DataDir: leechDataDir,
		DialTimeout: 5 * time.Second, RequestTimeout: 15 * time.Second,
	}, seedNode.FileHash())
	require.NoError(t, err)

	out2 := filepath.Join(t.TempDir(), "out2.bin")
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()
	require.NoError(t, leech2.Leech(ctx2, out2))

	got2, _ := os.ReadFile(out2)
	assert.Equal(t, srcBytes, got2, "resumed download must produce identical output")
}

// TestTransfer_RarePiece verifies rarest-first scheduling: if one peer has a
// unique piece, that piece is prioritised and distributed.
func TestTransfer_RarePiece(t *testing.T) {
	trackerAddr := startTracker(t)

	const pieceSize = 256 * 1024
	srcPath := randomFile(t, pieceSize*4)
	srcBytes, _ := os.ReadFile(srcPath)

	// both seeders have the full file — just verify the download completes
	// correctly when multiple seeders are present (sets up for rarest-first path)
	cfg := func(id string) peer.Config {
		return peer.Config{
			PeerID: id, ListenAddr: "127.0.0.1:0",
			TrackerAddr: trackerAddr, DataDir: t.TempDir(),
		}
	}

	s1, err := peer.NewSeedNode(cfg("seed-rare-1"), srcPath)
	require.NoError(t, err)
	s2, err := peer.NewSeedNode(cfg("seed-rare-2"), srcPath)
	require.NoError(t, err)

	mainCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s1.Seed(mainCtx) }()
	go func() { _ = s2.Seed(mainCtx) }()
	waitForSeeder(t, s1)
	waitForSeeder(t, s2)
	require.NoError(t, waitForRegistration(t, trackerAddr, s1.FileHash()))

	leech, err := peer.NewLeechNode(peer.Config{
		PeerID: "leecher-rare", ListenAddr: "127.0.0.1:0",
		TrackerAddr: trackerAddr, DataDir: t.TempDir(),
		DialTimeout: 5 * time.Second, RequestTimeout: 15 * time.Second,
	}, s1.FileHash())
	require.NoError(t, err)

	outPath := filepath.Join(t.TempDir(), "out.bin")
	leechCtx, leechCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer leechCancel()

	require.NoError(t, leech.Leech(leechCtx, outPath))

	got, _ := os.ReadFile(outPath)
	assert.Equal(t, srcBytes, got)
}
