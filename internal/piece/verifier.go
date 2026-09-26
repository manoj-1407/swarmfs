package piece

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Verify returns nil if the SHA-256 of data matches expectedHash (hex string).
// Any mismatch or encoding error is returned as an error.
func Verify(data []byte, expectedHash string) error {
	h := sha256.Sum256(data)
	got := hex.EncodeToString(h[:])
	if got != expectedHash {
		return fmt.Errorf("piece hash mismatch: want %s got %s", expectedHash, got)
	}
	return nil
}

// HashBytes returns the SHA-256 hex digest of data.
func HashBytes(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
