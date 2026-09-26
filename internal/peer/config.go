// Package peer implements a SwarmFS peer node — it seeds what it has
// and leeches what it needs simultaneously.
package peer

import "time"

// Config holds everything a Node needs to start.
type Config struct {
	PeerID      string
	ListenAddr  string
	TrackerAddr string
	DataDir     string

	// Adaptive enables network-aware peer selection in the downloader.
	// When false the downloader uses greedy first-available selection (static).
	Adaptive bool

	DialTimeout    time.Duration
	RequestTimeout time.Duration
}

func (c *Config) dialTimeout() time.Duration {
	if c.DialTimeout == 0 {
		return 10 * time.Second
	}
	return c.DialTimeout
}

func (c *Config) requestTimeout() time.Duration {
	if c.RequestTimeout == 0 {
		return 30 * time.Second
	}
	return c.RequestTimeout
}
