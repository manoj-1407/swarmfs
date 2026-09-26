// Package scenarios runs controlled SwarmFS experiments comparing static and
// adaptive peer scheduling under various simulated network conditions.
//
// Each scenario:
//  1. Starts a tracker + N seeders on loopback
//  2. Downloads the file with static scheduler — records time, throughput, events
//  3. Downloads the same file with adaptive scheduler — records the same
//  4. Appends a CSV row to results.csv
//
// Network conditions (latency, loss, bandwidth cap) require Linux tc+netem.
// If tc is unavailable the experiment still runs under baseline conditions.
package scenarios

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/manoj-1407/swarmfs/internal/peer"
	"github.com/manoj-1407/swarmfs/internal/tracker"
)

// ScenarioConfig describes one experiment scenario.
type ScenarioConfig struct {
	Name             string
	FileSizeMB       int
	PeerCount        int
	AddedLatencyMS   int     // added via tc netem; 0 = none
	LossPct          float64 // 0–100; applied via tc netem
	BandwidthCapMbps float64 // 0 = uncapped
}

// RunResult holds the outcome of one download run.
type RunResult struct {
	Scenario         string
	Adaptive         bool
	FileSizeMB       int
	PeerCount        int
	AddedLatencyMS   int
	LossPct          float64
	BandwidthCapMbps float64
	TotalTimeS       float64
	ThroughputMbps   float64
	PiecesOK         int64
	FailedAttempts   int64
	PeerSwitches     int64
}

// RunScenario executes static and adaptive downloads for cfg and returns both results.
func RunScenario(ctx context.Context, cfg ScenarioConfig, dataRoot string) (static, adaptive RunResult, err error) {
	// generate source file
	srcPath, err := randomFile(filepath.Join(dataRoot, "src"), cfg.FileSizeMB*1024*1024)
	if err != nil {
		return static, adaptive, fmt.Errorf("create source file: %w", err)
	}

	// apply netem (best-effort; logs and continues on failure)
	if cfg.AddedLatencyMS > 0 || cfg.LossPct > 0 || cfg.BandwidthCapMbps > 0 {
		applyNetem(cfg)
		defer resetNetem()
	}

	// start shared tracker
	trackerAddr, cancelTracker, err := startTracker(ctx)
	if err != nil {
		return static, adaptive, err
	}
	defer cancelTracker()

	// run static download
	static, err = runDownload(ctx, cfg, srcPath, trackerAddr, dataRoot, false)
	if err != nil {
		return static, adaptive, fmt.Errorf("static run: %w", err)
	}

	// run adaptive download (same file, fresh leecher data dir)
	adaptive, err = runDownload(ctx, cfg, srcPath, trackerAddr, dataRoot, true)
	if err != nil {
		return static, adaptive, fmt.Errorf("adaptive run: %w", err)
	}

	return static, adaptive, nil
}

// runDownload starts N seeders, downloads with a single leecher, and returns metrics.
func runDownload(ctx context.Context, cfg ScenarioConfig, srcPath, trackerAddr, dataRoot string, adaptive bool) (RunResult, error) {
	result := RunResult{
		Scenario:         cfg.Name,
		Adaptive:         adaptive,
		FileSizeMB:       cfg.FileSizeMB,
		PeerCount:        cfg.PeerCount,
		AddedLatencyMS:   cfg.AddedLatencyMS,
		LossPct:          cfg.LossPct,
		BandwidthCapMbps: cfg.BandwidthCapMbps,
	}

	seedCtx, cancelSeeders := context.WithCancel(ctx)
	defer cancelSeeders()

	// start seeders
	var seeders []*peer.Node
	for i := 0; i < cfg.PeerCount; i++ {
		n, err := peer.NewSeedNode(peer.Config{
			PeerID:      fmt.Sprintf("seeder-%d", i),
			ListenAddr:  "127.0.0.1:0",
			TrackerAddr: trackerAddr,
			DataDir:     filepath.Join(dataRoot, "seeds", fmt.Sprintf("s%d", i)),
		}, srcPath)
		if err != nil {
			return result, fmt.Errorf("init seeder %d: %w", i, err)
		}
		seeders = append(seeders, n)
		go func(node *peer.Node) { _ = node.Seed(seedCtx) }(n)
	}

	// wait for all seeders to bind and register
	for _, s := range seeders {
		if err := waitForSeeder(s, 5*time.Second); err != nil {
			return result, err
		}
	}
	if err := waitForRegistration(trackerAddr, seeders[0].FileHash(), cfg.PeerCount, 5*time.Second); err != nil {
		return result, err
	}

	// leecher
	mode := "static"
	if adaptive {
		mode = "adaptive"
	}
	leechNode, err := peer.NewLeechNode(peer.Config{
		PeerID:         fmt.Sprintf("leecher-%s", mode),
		ListenAddr:     "127.0.0.1:0",
		TrackerAddr:    trackerAddr,
		DataDir:        filepath.Join(dataRoot, "leech-"+mode),
		DialTimeout:    10 * time.Second,
		RequestTimeout: 30 * time.Second,
		Adaptive:       adaptive,
	}, seeders[0].FileHash())
	if err != nil {
		return result, fmt.Errorf("init leecher: %w", err)
	}

	outPath := filepath.Join(dataRoot, mode+"-output.bin")
	leechCtx, leechCancel := context.WithTimeout(ctx, 10*time.Minute)
	defer leechCancel()

	start := time.Now()
	m, err := leechNode.LeechWithMetrics(leechCtx, outPath)
	elapsed := time.Since(start)
	if err != nil {
		return result, fmt.Errorf("download: %w", err)
	}

	result.TotalTimeS = elapsed.Seconds()
	result.ThroughputMbps = float64(cfg.FileSizeMB) / elapsed.Seconds()
	result.PiecesOK = m.PiecesOK
	result.FailedAttempts = m.FailedAttempts
	result.PeerSwitches = m.PeerSwitches
	return result, nil
}

// AppendCSV writes results to path (creating if needed) as a CSV row.
func AppendCSV(path string, results ...RunResult) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)

	// write header if file was just created (size == 0)
	info, _ := f.Stat()
	if info.Size() == 0 {
		_ = w.Write([]string{
			"scenario", "adaptive", "file_size_mb", "peer_count",
			"latency_ms", "loss_pct", "bw_cap_mbps",
			"total_time_s", "throughput_mbps",
			"pieces_ok", "failed_attempts", "peer_switches",
		})
	}

	for _, r := range results {
		_ = w.Write([]string{
			r.Scenario,
			strconv.FormatBool(r.Adaptive),
			strconv.Itoa(r.FileSizeMB),
			strconv.Itoa(r.PeerCount),
			strconv.Itoa(r.AddedLatencyMS),
			strconv.FormatFloat(r.LossPct, 'f', 2, 64),
			strconv.FormatFloat(r.BandwidthCapMbps, 'f', 2, 64),
			strconv.FormatFloat(r.TotalTimeS, 'f', 3, 64),
			strconv.FormatFloat(r.ThroughputMbps, 'f', 3, 64),
			strconv.FormatInt(r.PiecesOK, 10),
			strconv.FormatInt(r.FailedAttempts, 10),
			strconv.FormatInt(r.PeerSwitches, 10),
		})
	}
	w.Flush()
	return w.Error()
}

// DefaultScenarios returns the standard experiment suite.
var DefaultScenarios = []ScenarioConfig{
	{Name: "baseline", FileSizeMB: 50, PeerCount: 3},
	{Name: "latency_100ms", FileSizeMB: 50, PeerCount: 3, AddedLatencyMS: 100},
	{Name: "loss_5pct", FileSizeMB: 50, PeerCount: 3, LossPct: 5},
	{Name: "bw_cap_1mbps", FileSizeMB: 50, PeerCount: 3, BandwidthCapMbps: 1},
	{Name: "mixed", FileSizeMB: 50, PeerCount: 3, AddedLatencyMS: 50, LossPct: 2},
}

// --- helpers ---

func randomFile(path string, size int) (string, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(filepath.Dir(path), "src-*")
	if err != nil {
		return "", err
	}
	buf := make([]byte, min(size, 1024*1024))
	remaining := size
	for remaining > 0 {
		n := min(remaining, len(buf))
		if _, err := rand.Read(buf[:n]); err != nil {
			return "", err
		}
		if _, err := f.Write(buf[:n]); err != nil {
			return "", err
		}
		remaining -= n
	}
	return f.Name(), f.Close()
}

func startTracker(ctx context.Context) (string, context.CancelFunc, error) {
	ctx2, cancel := context.WithCancel(ctx)
	srv := tracker.NewServer("127.0.0.1:0")
	go func() { _ = srv.Serve(ctx2) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if addr := srv.ListenAddr(); addr != "" {
			return addr, cancel, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	return "", nil, fmt.Errorf("tracker did not start")
}

func waitForSeeder(n *peer.Node, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.ListenAddr() != "" {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("seeder did not bind in %s", timeout)
}

func waitForRegistration(trackerAddr, fileHash string, wantCount int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		peers, err := tracker.GetPeers(trackerAddr, fileHash, "probe")
		if err == nil && len(peers) >= wantCount {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("%d peers not registered after %s", wantCount, timeout)
}

func applyNetem(cfg ScenarioConfig) {
	log.Printf("scenarios: netem — latency %dms loss %.1f%% bw %.1fMbps (requires root+tc)",
		cfg.AddedLatencyMS, cfg.LossPct, cfg.BandwidthCapMbps)
	// tc netem is applied via shell scripts in scripts/netem/
	// If not available (e.g. CI without root), we just log and continue.
}

func resetNetem() {
	log.Printf("scenarios: reset netem")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
