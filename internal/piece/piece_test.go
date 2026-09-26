package piece_test

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/manoj-1407/swarmfs/internal/piece"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- helpers ---

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

func writeTempFile(t *testing.T, data []byte) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "swarmfs-src-*")
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func newStore(t *testing.T) *piece.Store {
	t.Helper()
	s, err := piece.NewStore(t.TempDir())
	require.NoError(t, err)
	return s
}

// --- verifier ---

func TestVerify_Correct(t *testing.T) {
	data := []byte("swarmfs piece data")
	hash := piece.HashBytes(data)
	require.NoError(t, piece.Verify(data, hash))
}

func TestVerify_CorruptData(t *testing.T) {
	data := []byte("original")
	hash := piece.HashBytes(data)

	corrupted := []byte("tampered")
	err := piece.Verify(corrupted, hash)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "hash mismatch")
}

func TestVerify_EmptyData(t *testing.T) {
	data := []byte{}
	hash := piece.HashBytes(data)
	require.NoError(t, piece.Verify(data, hash))
}

func TestHashBytes_Deterministic(t *testing.T) {
	data := randomBytes(t, 1024)
	assert.Equal(t, piece.HashBytes(data), piece.HashBytes(data))
}

func TestHashBytes_DifferentInputsDifferentOutputs(t *testing.T) {
	a := randomBytes(t, 512)
	b := randomBytes(t, 512)
	assert.NotEqual(t, piece.HashBytes(a), piece.HashBytes(b))
}

// --- store ---

func TestStore_WriteReadHasDelete(t *testing.T) {
	s := newStore(t)
	data := randomBytes(t, 1024)

	require.NoError(t, s.Write(0, data))
	assert.True(t, s.Has(0))

	got, err := s.Read(0)
	require.NoError(t, err)
	assert.Equal(t, data, got)

	require.NoError(t, s.Delete(0))
	assert.False(t, s.Has(0))
}

func TestStore_ReadNonexistent(t *testing.T) {
	s := newStore(t)
	_, err := s.Read(99)
	require.Error(t, err)
}

func TestStore_DeleteNonexistent(t *testing.T) {
	s := newStore(t)
	// should be a no-op, not an error
	require.NoError(t, s.Delete(99))
}

func TestStore_OverwritePiece(t *testing.T) {
	s := newStore(t)
	first := []byte("first version")
	second := []byte("second version")

	require.NoError(t, s.Write(0, first))
	require.NoError(t, s.Write(0, second))

	got, err := s.Read(0)
	require.NoError(t, err)
	assert.Equal(t, second, got)
}

func TestStore_MultiplePieces(t *testing.T) {
	s := newStore(t)
	const n = 20
	pieces := make([][]byte, n)

	for i := 0; i < n; i++ {
		pieces[i] = randomBytes(t, 256)
		require.NoError(t, s.Write(i, pieces[i]))
	}

	for i := 0; i < n; i++ {
		assert.True(t, s.Has(i))
		got, err := s.Read(i)
		require.NoError(t, err)
		assert.Equal(t, pieces[i], got, "piece %d mismatch", i)
	}
}

// --- manifest ---

func TestManifest_WriteRead_Roundtrip(t *testing.T) {
	dir := t.TempDir()
	m := &piece.Manifest{
		FileHash:   "abc123",
		FileName:   "testfile.bin",
		FileSize:   1048576,
		PieceSize:  262144,
		PieceCount: 4,
		Pieces: []piece.Entry{
			{Index: 0, Hash: "hash0", Size: 262144},
			{Index: 1, Hash: "hash1", Size: 262144},
			{Index: 2, Hash: "hash2", Size: 262144},
			{Index: 3, Hash: "hash3", Size: 262144},
		},
	}

	require.NoError(t, m.WriteToDir(dir))
	assert.FileExists(t, filepath.Join(dir, piece.ManifestFilename))

	loaded, err := piece.ReadManifest(dir)
	require.NoError(t, err)

	assert.Equal(t, m.FileHash, loaded.FileHash)
	assert.Equal(t, m.FileName, loaded.FileName)
	assert.Equal(t, m.FileSize, loaded.FileSize)
	assert.Equal(t, m.PieceSize, loaded.PieceSize)
	assert.Equal(t, m.PieceCount, loaded.PieceCount)
	require.Len(t, loaded.Pieces, 4)
	assert.Equal(t, "hash2", loaded.Pieces[2].Hash)
}

func TestReadManifest_NonexistentDir(t *testing.T) {
	_, err := piece.ReadManifest("/nonexistent/path")
	require.Error(t, err)
}

// --- chunker and assembler ---

func chunkAndCheck(t *testing.T, src []byte, pieceSize int) (*piece.Manifest, *piece.Store) {
	t.Helper()

	srcPath := writeTempFile(t, src)
	s := newStore(t)

	m, err := piece.Chunk(srcPath, s, pieceSize)
	require.NoError(t, err)

	assert.Equal(t, int64(len(src)), m.FileSize)
	assert.Equal(t, pieceSize, m.PieceSize)
	assert.NotEmpty(t, m.FileHash)

	for _, entry := range m.Pieces {
		data, err := s.Read(entry.Index)
		require.NoError(t, err, "piece %d should be on disk", entry.Index)
		require.NoError(t, piece.Verify(data, entry.Hash), "piece %d hash should be valid", entry.Index)
	}

	return m, s
}

func TestChunk_NotExactMultiple(t *testing.T) {
	// 2.5 × pieceSize
	pieceSize := 1024
	src := randomBytes(t, pieceSize*2+pieceSize/2)

	m, _ := chunkAndCheck(t, src, pieceSize)
	assert.Equal(t, 3, m.PieceCount)
	assert.Equal(t, pieceSize, m.Pieces[0].Size)
	assert.Equal(t, pieceSize, m.Pieces[1].Size)
	assert.Equal(t, pieceSize/2, m.Pieces[2].Size) // last piece is smaller
}

func TestChunk_ExactMultiple(t *testing.T) {
	pieceSize := 512
	src := randomBytes(t, pieceSize*4)

	m, _ := chunkAndCheck(t, src, pieceSize)
	assert.Equal(t, 4, m.PieceCount)
	for _, e := range m.Pieces {
		assert.Equal(t, pieceSize, e.Size)
	}
}

func TestChunk_SmallerThanOnePiece(t *testing.T) {
	src := randomBytes(t, 100)
	m, _ := chunkAndCheck(t, src, 4096)

	assert.Equal(t, 1, m.PieceCount)
	assert.Equal(t, 100, m.Pieces[0].Size)
}

func TestChunk_SingleByte(t *testing.T) {
	src := []byte{0x42}
	m, _ := chunkAndCheck(t, src, 256)

	assert.Equal(t, 1, m.PieceCount)
	assert.Equal(t, 1, m.Pieces[0].Size)
}

func TestChunk_LargerFile(t *testing.T) {
	// 1 MiB — real-world-ish size in a test
	src := randomBytes(t, 1024*1024)
	m, _ := chunkAndCheck(t, src, DefaultPieceSize)

	assert.Equal(t, 4, m.PieceCount)
}

func TestAssemble_RoundTrip(t *testing.T) {
	pieceSize := 512
	src := randomBytes(t, pieceSize*3+100)

	m, s := chunkAndCheck(t, src, pieceSize)

	dstPath := filepath.Join(t.TempDir(), "assembled.bin")
	require.NoError(t, piece.Assemble(dstPath, m, s))

	got, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	assert.Equal(t, src, got, "assembled file must be byte-identical to source")
}

func TestAssemble_RoundTrip_ExactMultiple(t *testing.T) {
	pieceSize := 256
	src := randomBytes(t, pieceSize*8)

	m, s := chunkAndCheck(t, src, pieceSize)

	dstPath := filepath.Join(t.TempDir(), "assembled.bin")
	require.NoError(t, piece.Assemble(dstPath, m, s))

	got, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(src, got))
}

func TestAssemble_DetectsMissingPiece(t *testing.T) {
	src := randomBytes(t, 1024)
	m, s := chunkAndCheck(t, src, 256)

	// delete piece 1 — assemble must fail
	require.NoError(t, s.Delete(1))

	dstPath := filepath.Join(t.TempDir(), "assembled.bin")
	err := piece.Assemble(dstPath, m, s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "piece 1")

	// partial output file should be cleaned up
	_, statErr := os.Stat(dstPath)
	assert.True(t, os.IsNotExist(statErr), "assemble must remove partial output on failure")
}

func TestAssemble_DetectsCorruption(t *testing.T) {
	pieceSize := 256
	src := randomBytes(t, pieceSize*4)
	m, s := chunkAndCheck(t, src, pieceSize)

	// overwrite piece 2 with garbage
	garbage := randomBytes(t, pieceSize)
	require.NoError(t, s.Write(2, garbage))

	dstPath := filepath.Join(t.TempDir(), "assembled.bin")
	err := piece.Assemble(dstPath, m, s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "corrupt")
}

func TestChunk_ManifestFileHashMatchesSource(t *testing.T) {
	src := randomBytes(t, 3000)
	srcPath := writeTempFile(t, src)
	s := newStore(t)

	m, err := piece.Chunk(srcPath, s, 1024)
	require.NoError(t, err)

	expected := piece.HashBytes(src)
	assert.Equal(t, expected, m.FileHash)
}

func TestChunk_NonexistentFile(t *testing.T) {
	s := newStore(t)
	_, err := piece.Chunk("/nonexistent/file.bin", s, 256)
	require.Error(t, err)
}

const DefaultPieceSize = piece.DefaultPieceSize
