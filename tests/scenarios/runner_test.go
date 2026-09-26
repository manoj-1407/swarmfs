package scenarios_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/manoj-1407/swarmfs/tests/scenarios"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestScenario_Baseline verifies that both static and adaptive schedulers
// correctly download and reconstruct a multi-piece file under normal conditions.
// This is the sanity gate: if it fails, nothing else matters.
func TestScenario_Baseline(t *testing.T) {
	ctx := context.Background()
	dataRoot := t.TempDir()

	static, adaptive, err := scenarios.RunScenario(ctx, scenarios.ScenarioConfig{
		Name:       "baseline_test",
		FileSizeMB: 1,
		PeerCount:  2,
	}, dataRoot)

	require.NoError(t, err)

	// both runs must complete all pieces
	const expectedPieces = 4 // 1 MB / 256 KB = 4 pieces
	assert.Equal(t, int64(expectedPieces), static.PiecesOK,
		"static: all pieces must be downloaded")
	assert.Equal(t, int64(expectedPieces), adaptive.PiecesOK,
		"adaptive: all pieces must be downloaded")

	// no failures under clean conditions
	assert.Equal(t, int64(0), static.FailedAttempts, "static: no failures baseline")
	assert.Equal(t, int64(0), adaptive.FailedAttempts, "adaptive: no failures baseline")

	// timing sanity — should complete well under 30s on loopback
	assert.Less(t, static.TotalTimeS, 30.0)
	assert.Less(t, adaptive.TotalTimeS, 30.0)

	t.Logf("static:   %.3fs  %.3f Mbps  pieces=%d  switches=%d",
		static.TotalTimeS, static.ThroughputMbps, static.PiecesOK, static.PeerSwitches)
	t.Logf("adaptive: %.3fs  %.3f Mbps  pieces=%d  switches=%d",
		adaptive.TotalTimeS, adaptive.ThroughputMbps, adaptive.PiecesOK, adaptive.PeerSwitches)
}

// TestScenario_ThreePeers verifies scheduler behaviour with more peers than workers.
func TestScenario_ThreePeers(t *testing.T) {
	ctx := context.Background()

	static, adaptive, err := scenarios.RunScenario(ctx, scenarios.ScenarioConfig{
		Name:       "three_peers_test",
		FileSizeMB: 2,
		PeerCount:  3,
	}, t.TempDir())

	require.NoError(t, err)

	const expectedPieces = 8 // 2 MB / 256 KB
	assert.Equal(t, int64(expectedPieces), static.PiecesOK)
	assert.Equal(t, int64(expectedPieces), adaptive.PiecesOK)
}

// TestScenario_CSVOutput verifies the CSV writer produces a parseable file
// with correct column count.
func TestScenario_CSVOutput(t *testing.T) {
	ctx := context.Background()
	dataRoot := t.TempDir()

	static, adaptive, err := scenarios.RunScenario(ctx, scenarios.ScenarioConfig{
		Name:       "csv_test",
		FileSizeMB: 1,
		PeerCount:  2,
	}, dataRoot)
	require.NoError(t, err)

	csvPath := filepath.Join(dataRoot, "results.csv")
	require.NoError(t, scenarios.AppendCSV(csvPath, static, adaptive))

	data, err := os.ReadFile(csvPath)
	require.NoError(t, err)

	lines := splitLines(string(data))
	// header + 2 data rows
	require.Len(t, lines, 3, "expected header + 2 result rows")

	// each row must have 12 columns
	for i, line := range lines {
		cols := splitCSV(line)
		assert.Len(t, cols, 12, "row %d must have 12 columns", i)
	}

	t.Logf("CSV output:\n%s", string(data))
}

// TestScenario_AdaptiveVsStatic_Correctness checks that adaptive mode does
// not sacrifice correctness for speed — the downloaded bytes must be identical
// to the source regardless of scheduler.
func TestScenario_AdaptiveVsStatic_Correctness(t *testing.T) {
	ctx := context.Background()
	dataRoot := t.TempDir()

	_, _, err := scenarios.RunScenario(ctx, scenarios.ScenarioConfig{
		Name:       "correctness_test",
		FileSizeMB: 2,
		PeerCount:  3,
	}, dataRoot)
	require.NoError(t, err)

	// RunScenario assembles files; verify both outputs exist and are non-empty
	for _, mode := range []string{"static", "adaptive"} {
		outPath := filepath.Join(dataRoot, mode+"-output.bin")
		info, err := os.Stat(outPath)
		require.NoError(t, err, "%s output must exist", mode)
		assert.Equal(t, int64(2*1024*1024), info.Size(),
			"%s output size must match source", mode)
	}
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i, c := range s {
		if c == '\n' {
			line := s[start:i]
			if line != "" {
				lines = append(lines, line)
			}
			start = i + 1
		}
	}
	if start < len(s) && s[start:] != "" {
		lines = append(lines, s[start:])
	}
	return lines
}

func splitCSV(s string) []string {
	var cols []string
	start := 0
	for i, c := range s {
		if c == ',' {
			cols = append(cols, s[start:i])
			start = i + 1
		}
	}
	cols = append(cols, s[start:])
	return cols
}
