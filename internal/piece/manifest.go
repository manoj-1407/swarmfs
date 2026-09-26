// Package piece handles file chunking, verification, on-disk storage,
// and manifest generation/parsing.
package piece

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// ManifestFilename is the fixed name for manifest files in a piece store directory.
const ManifestFilename = "manifest.json"

// Entry describes a single piece within a file.
type Entry struct {
	Index int    `json:"index"`
	Hash  string `json:"hash"` // sha256 hex of the piece bytes
	Size  int    `json:"size"` // actual byte count (last piece may be smaller)
}

// Manifest describes a complete file that has been split into pieces.
// It is small enough to transfer before any piece data begins.
type Manifest struct {
	FileHash   string  `json:"fileHash"`   // sha256 hex of the original file
	FileName   string  `json:"fileName"`   // original basename
	FileSize   int64   `json:"fileSize"`   // total bytes
	PieceSize  int     `json:"pieceSize"`  // nominal piece size in bytes
	PieceCount int     `json:"pieceCount"` // number of pieces
	Pieces     []Entry `json:"pieces"`
}

// WriteToDir serialises the manifest to dir/manifest.json.
// dir must already exist.
func (m *Manifest) WriteToDir(dir string) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("manifest marshal: %w", err)
	}
	path := filepath.Join(dir, ManifestFilename)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("manifest write to %s: %w", path, err)
	}
	return nil
}

// ReadManifest loads and parses dir/manifest.json.
func ReadManifest(dir string) (*Manifest, error) {
	path := filepath.Join(dir, ManifestFilename)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("manifest read from %s: %w", path, err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("manifest parse: %w", err)
	}
	return &m, nil
}
