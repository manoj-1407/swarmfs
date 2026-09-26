package protocol

import (
	"encoding/hex"
	"fmt"
	"sync"
)

// Bitfield tracks which pieces a peer has.
// Bit i (0-indexed, MSB-first within each byte) is set if piece i is present.
//
//	byte 0:  pieces 0–7   (bit 7 of byte 0 = piece 0)
//	byte 1:  pieces 8–15
//	...
//
// All methods are safe for concurrent use.
type Bitfield struct {
	mu    sync.RWMutex
	data  []byte
	total int
}

// NewBitfield creates an empty Bitfield for pieceCount pieces.
func NewBitfield(pieceCount int) *Bitfield {
	if pieceCount < 0 {
		pieceCount = 0
	}
	return &Bitfield{
		data:  make([]byte, (pieceCount+7)/8),
		total: pieceCount,
	}
}

// Set marks piece index as available. Returns an error if index is out of range.
func (b *Bitfield) Set(index int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if index < 0 || index >= b.total {
		return fmt.Errorf("bitfield.Set: index %d out of range [0, %d)", index, b.total)
	}
	byteIdx := index / 8
	bitIdx := uint(7 - (index % 8))
	b.data[byteIdx] |= 1 << bitIdx
	return nil
}

// Has reports whether piece index is available.
func (b *Bitfield) Has(index int) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if index < 0 || index >= b.total {
		return false
	}
	byteIdx := index / 8
	bitIdx := uint(7 - (index % 8))
	return b.data[byteIdx]&(1<<bitIdx) != 0
}

// Encode returns the hex-encoded wire representation.
func (b *Bitfield) Encode() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return hex.EncodeToString(b.data)
}

// Total returns the number of pieces this bitfield tracks.
func (b *Bitfield) Total() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.total
}

// Count returns how many pieces are currently marked as available.
func (b *Bitfield) Count() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.countLocked()
}

func (b *Bitfield) countLocked() int {
	n := 0
	for i := 0; i < b.total; i++ {
		byteIdx := i / 8
		bitIdx := uint(7 - (i % 8))
		if b.data[byteIdx]&(1<<bitIdx) != 0 {
			n++
		}
	}
	return n
}

// Missing returns the indices of all pieces not yet available.
// The returned slice is a snapshot — safe to use after the call returns.
func (b *Bitfield) Missing() []int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	missing := make([]int, 0)
	for i := 0; i < b.total; i++ {
		byteIdx := i / 8
		bitIdx := uint(7 - (i % 8))
		if b.data[byteIdx]&(1<<bitIdx) == 0 {
			missing = append(missing, i)
		}
	}
	return missing
}

// Complete reports whether all pieces are available.
func (b *Bitfield) Complete() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for i := 0; i < b.total; i++ {
		byteIdx := i / 8
		bitIdx := uint(7 - (i % 8))
		if b.data[byteIdx]&(1<<bitIdx) == 0 {
			return false
		}
	}
	return true
}

// DecodeBitfield parses a hex-encoded bitfield from the wire.
func DecodeBitfield(encoded string, pieceCount int) (*Bitfield, error) {
	data, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("bitfield hex decode: %w", err)
	}
	expected := (pieceCount + 7) / 8
	if len(data) != expected {
		return nil, fmt.Errorf("bitfield length mismatch: got %d bytes, want %d (for %d pieces)",
			len(data), expected, pieceCount)
	}
	return &Bitfield{data: data, total: pieceCount}, nil
}
