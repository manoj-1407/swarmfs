package piece

import (
	"fmt"
	"os"
	"path/filepath"
)

// Store manages piece files on disk inside a single directory.
// Pieces are stored as {dir}/{index:04d}.piece.
//
// Store is not safe for concurrent use; callers must synchronise externally.
type Store struct {
	dir string
}

// NewStore creates (if needed) and returns a Store rooted at dir.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: create dir %s: %w", dir, err)
	}
	return &Store{dir: dir}, nil
}

// Write persists data as piece index. Overwrites any existing file.
func (s *Store) Write(index int, data []byte) error {
	if err := os.WriteFile(s.path(index), data, 0o644); err != nil {
		return fmt.Errorf("store: write piece %d: %w", index, err)
	}
	return nil
}

// Read loads piece index from disk. Returns an error if the piece does not exist.
func (s *Store) Read(index int) ([]byte, error) {
	data, err := os.ReadFile(s.path(index))
	if err != nil {
		return nil, fmt.Errorf("store: read piece %d: %w", index, err)
	}
	return data, nil
}

// Has reports whether piece index is present on disk.
func (s *Store) Has(index int) bool {
	_, err := os.Stat(s.path(index))
	return err == nil
}

// Delete removes piece index from disk. No-op if the piece does not exist.
func (s *Store) Delete(index int) error {
	err := os.Remove(s.path(index))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("store: delete piece %d: %w", index, err)
	}
	return nil
}

// Dir returns the root directory of this store.
func (s *Store) Dir() string {
	return s.dir
}

func (s *Store) path(index int) string {
	return filepath.Join(s.dir, fmt.Sprintf("%04d.piece", index))
}
