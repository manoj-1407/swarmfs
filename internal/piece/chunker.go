package piece

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// DefaultPieceSize is 256 KiB — a reasonable default for LAN transfers.
const DefaultPieceSize = 256 * 1024

// Chunk splits the file at srcPath into pieces, writes them to store,
// and returns the Manifest. Use pieceSize <= 0 to get DefaultPieceSize.
//
// All pieces are SHA-256 verified at write time. The manifest's FileHash
// covers the entire original file.
func Chunk(srcPath string, store *Store, pieceSize int) (*Manifest, error) {
	if pieceSize <= 0 {
		pieceSize = DefaultPieceSize
	}

	f, err := os.Open(srcPath)
	if err != nil {
		return nil, fmt.Errorf("chunk: open %s: %w", srcPath, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("chunk: stat %s: %w", srcPath, err)
	}

	// hash the entire file first
	fileHasher := sha256.New()
	if _, err := io.Copy(fileHasher, f); err != nil {
		return nil, fmt.Errorf("chunk: hash file: %w", err)
	}
	fileHash := hex.EncodeToString(fileHasher.Sum(nil))

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("chunk: seek to start: %w", err)
	}

	var entries []Entry
	buf := make([]byte, pieceSize)
	index := 0

	for {
		n, readErr := io.ReadFull(f, buf)
		if n > 0 {
			data := buf[:n]
			hash := HashBytes(data)

			if err := store.Write(index, data); err != nil {
				return nil, fmt.Errorf("chunk: store piece %d: %w", index, err)
			}

			entries = append(entries, Entry{
				Index: index,
				Hash:  hash,
				Size:  n,
			})
			index++
		}

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("chunk: read piece %d: %w", index, readErr)
		}
	}

	return &Manifest{
		FileHash:   fileHash,
		FileName:   filepath.Base(srcPath),
		FileSize:   info.Size(),
		PieceSize:  pieceSize,
		PieceCount: len(entries),
		Pieces:     entries,
	}, nil
}

// Assemble reconstructs a file from pieces in store according to manifest
// and writes the result to dstPath.
//
// Each piece is verified against its manifest hash before being written.
// After all pieces are assembled, the complete file hash is verified against
// manifest.FileHash. Any mismatch returns an error — no partial output is kept.
func Assemble(dstPath string, manifest *Manifest, store *Store) error {
	f, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("assemble: create %s: %w", dstPath, err)
	}

	// clean up the partial file on any error
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(dstPath)
		}
	}()

	fileHasher := sha256.New()

	for _, entry := range manifest.Pieces {
		data, err := store.Read(entry.Index)
		if err != nil {
			return fmt.Errorf("assemble: piece %d missing: %w", entry.Index, err)
		}

		if err := Verify(data, entry.Hash); err != nil {
			return fmt.Errorf("assemble: piece %d corrupt: %w", entry.Index, err)
		}

		if _, err := f.Write(data); err != nil {
			return fmt.Errorf("assemble: write piece %d: %w", entry.Index, err)
		}

		fileHasher.Write(data)
	}

	got := hex.EncodeToString(fileHasher.Sum(nil))
	if got != manifest.FileHash {
		return fmt.Errorf("assemble: file hash mismatch: want %s got %s", manifest.FileHash, got)
	}

	ok = true
	return nil
}
