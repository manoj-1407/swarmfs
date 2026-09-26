package metrics_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/manoj-1407/swarmfs/pkg/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollector_PiecesSummary(t *testing.T) {
	c := metrics.NewCollector()

	for i := 0; i < 4; i++ {
		c.RecordPiece("peer:9001", i, 256*1024, 50*time.Millisecond, 5.0, 50.0)
	}

	s := c.Summary()
	assert.Equal(t, int64(4), s.Pieces)
	assert.Equal(t, int64(4*256*1024), s.Bytes)
	assert.Equal(t, int64(0), s.Switches)
	assert.Equal(t, int64(0), s.Failed)
	assert.Greater(t, s.TputMbps, 0.0)
}

func TestCollector_SwitchAndFail(t *testing.T) {
	c := metrics.NewCollector()
	c.RecordSwitch("peer:9001")
	c.RecordSwitch("peer:9002")
	c.RecordFail("peer:9003", 7)

	s := c.Summary()
	assert.Equal(t, int64(2), s.Switches)
	assert.Equal(t, int64(1), s.Failed)
}

func TestCollector_WriteEvents_CSV(t *testing.T) {
	c := metrics.NewCollector()
	c.RecordPiece("p1:9001", 0, 1024, 10*time.Millisecond, 2.5, 100.0)
	c.RecordSwitch("p1:9001")
	c.RecordProbe("p2:9002", 15.0, true)
	c.RecordProbe("p2:9002", 0.0, false)

	var buf bytes.Buffer
	require.NoError(t, c.WriteEvents(&buf))

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	// header + 4 events
	assert.Len(t, lines, 5)

	// header columns
	assert.Equal(t, "elapsed_ms,type,peer,piece_idx,bytes,duration_ms,rtt_ms,tput_mbps", lines[0])

	// piece row
	assert.Contains(t, lines[1], "piece")
	assert.Contains(t, lines[1], "p1:9001")

	// switch row
	assert.Contains(t, lines[2], "switch")
}

func TestCollector_Concurrent(t *testing.T) {
	c := metrics.NewCollector()

	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func(i int) {
			for j := 0; j < 100; j++ {
				c.RecordPiece("p:9000", i*100+j, 256, time.Millisecond, 1.0, 1.0)
			}
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 8; i++ {
		<-done
	}

	s := c.Summary()
	assert.Equal(t, int64(800), s.Pieces)
}
