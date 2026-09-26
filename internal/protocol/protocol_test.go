package protocol_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"testing"

	"github.com/manoj-1407/swarmfs/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- encoder tests ---

func TestEncodeDecode_Roundtrip(t *testing.T) {
	tests := []struct {
		name    string
		msgType protocol.MessageType
		payload any
		decoded any
		check   func(t *testing.T, decoded any)
	}{
		{
			name:    "register",
			msgType: protocol.TypeRegister,
			payload: protocol.RegisterPayload{
				FileHash: "deadbeef",
				PeerAddr: "127.0.0.1:9001",
				Bitfield: "ff00",
			},
			decoded: &protocol.RegisterPayload{},
			check: func(t *testing.T, d any) {
				p := d.(*protocol.RegisterPayload)
				assert.Equal(t, "deadbeef", p.FileHash)
				assert.Equal(t, "127.0.0.1:9001", p.PeerAddr)
				assert.Equal(t, "ff00", p.Bitfield)
			},
		},
		{
			name:    "peer_list",
			msgType: protocol.TypePeerList,
			payload: protocol.PeerListPayload{
				Peers: []protocol.PeerInfo{
					{Addr: "127.0.0.1:9001", Bitfield: "ff"},
					{Addr: "127.0.0.1:9002", Bitfield: "0f"},
				},
			},
			decoded: &protocol.PeerListPayload{},
			check: func(t *testing.T, d any) {
				p := d.(*protocol.PeerListPayload)
				require.Len(t, p.Peers, 2)
				assert.Equal(t, "127.0.0.1:9001", p.Peers[0].Addr)
				assert.Equal(t, "0f", p.Peers[1].Bitfield)
			},
		},
		{
			name:    "have",
			msgType: protocol.TypeHave,
			payload: protocol.HavePayload{
				FileHash:   "abc123",
				PieceIndex: 42,
				PeerAddr:   "10.0.0.1:8080",
			},
			decoded: &protocol.HavePayload{},
			check: func(t *testing.T, d any) {
				p := d.(*protocol.HavePayload)
				assert.Equal(t, 42, p.PieceIndex)
			},
		},
		{
			name:    "handshake",
			msgType: protocol.TypeHandshake,
			payload: protocol.HandshakePayload{
				PeerID:   "peer-xyz",
				FileHash: "filehash",
				Bitfield: "00",
			},
			decoded: &protocol.HandshakePayload{},
			check: func(t *testing.T, d any) {
				p := d.(*protocol.HandshakePayload)
				assert.Equal(t, "peer-xyz", p.PeerID)
			},
		},
		{
			name:    "piece",
			msgType: protocol.TypePiece,
			payload: protocol.PiecePayload{
				PieceIndex: 7,
				Data:       []byte("the quick brown fox jumps over the lazy dog"),
				Hash:       "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592",
			},
			decoded: &protocol.PiecePayload{},
			check: func(t *testing.T, d any) {
				p := d.(*protocol.PiecePayload)
				assert.Equal(t, 7, p.PieceIndex)
				assert.Equal(t, []byte("the quick brown fox jumps over the lazy dog"), p.Data)
			},
		},
		{
			name:    "probe",
			msgType: protocol.TypeProbe,
			payload: protocol.ProbePayload{Seq: 99, SentAt: 1700000000000},
			decoded: &protocol.ProbePayload{},
			check: func(t *testing.T, d any) {
				p := d.(*protocol.ProbePayload)
				assert.Equal(t, uint64(99), p.Seq)
				assert.Equal(t, int64(1700000000000), p.SentAt)
			},
		},
		{
			name:    "probe_ack",
			msgType: protocol.TypeProbeAck,
			payload: protocol.ProbeAckPayload{Seq: 5, SentAt: 1000, AckedAt: 1050},
			decoded: &protocol.ProbeAckPayload{},
			check: func(t *testing.T, d any) {
				p := d.(*protocol.ProbeAckPayload)
				assert.Equal(t, uint64(5), p.Seq)
				assert.Equal(t, int64(1050), p.AckedAt)
			},
		},
		{
			name:    "leave",
			msgType: protocol.TypeLeave,
			payload: protocol.LeavePayload{PeerAddr: "127.0.0.1:9001", FileHash: "abc"},
			decoded: &protocol.LeavePayload{},
			check: func(t *testing.T, d any) {
				p := d.(*protocol.LeavePayload)
				assert.Equal(t, "127.0.0.1:9001", p.PeerAddr)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			err := protocol.Encode(&buf, tt.msgType, tt.payload)
			require.NoError(t, err)
			assert.Greater(t, buf.Len(), 4, "encoded output must be more than just the length prefix")

			msg, err := protocol.Decode(&buf)
			require.NoError(t, err)
			assert.Equal(t, tt.msgType, msg.Type)
			assert.Equal(t, "1", msg.Version)

			err = protocol.UnmarshalPayload(msg, tt.decoded)
			require.NoError(t, err)

			tt.check(t, tt.decoded)
		})
	}
}

func TestDecode_ZeroLength(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x00, 0x00, 0x00})
	_, err := protocol.Decode(&buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "zero-length")
}

func TestDecode_MessageTooLarge(t *testing.T) {
	var buf bytes.Buffer
	// claim 33 MB
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], 33*1024*1024+1)
	buf.Write(prefix[:])
	_, err := protocol.Decode(&buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too large")
}

func TestDecode_WrongVersion(t *testing.T) {
	msg := `{"type":"HEARTBEAT","v":"99","p":{"peerAddr":"x"}}`
	var buf bytes.Buffer
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(msg)))
	buf.Write(prefix[:])
	buf.WriteString(msg)

	_, err := protocol.Decode(&buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported protocol version")
}

func TestDecode_TruncatedBody(t *testing.T) {
	var buf bytes.Buffer
	// say 100 bytes but only write 10
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], 100)
	buf.Write(prefix[:])
	buf.WriteString("short data")

	_, err := protocol.Decode(&buf)
	require.Error(t, err)
}

func TestEncode_LargePayloadRejected(t *testing.T) {
	// PiecePayload with data > 32 MB should be rejected
	huge := make([]byte, 33*1024*1024)
	err := protocol.Encode(&bytes.Buffer{}, protocol.TypePiece, protocol.PiecePayload{
		PieceIndex: 0,
		Data:       huge,
		Hash:       "x",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds limit")
}

func TestConn_SendRecv(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	server := protocol.NewConn(serverConn)
	client := protocol.NewConn(clientConn)

	want := protocol.HandshakePayload{
		PeerID:   "peer-alpha",
		FileHash: "cafebabe",
		Bitfield: "ff",
	}

	sendErr := make(chan error, 1)
	go func() {
		sendErr <- client.Send(protocol.TypeHandshake, want)
	}()

	msg, err := server.Recv()
	require.NoError(t, err)
	require.NoError(t, <-sendErr)

	assert.Equal(t, protocol.TypeHandshake, msg.Type)

	var got protocol.HandshakePayload
	require.NoError(t, protocol.UnmarshalPayload(msg, &got))
	assert.Equal(t, want.PeerID, got.PeerID)
	assert.Equal(t, want.FileHash, got.FileHash)
	assert.Equal(t, want.Bitfield, got.Bitfield)
}

func TestConn_MultipleMessages(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	connA := protocol.NewConn(a)
	connB := protocol.NewConn(b)

	const count = 10
	done := make(chan error, 1)

	go func() {
		for i := 0; i < count; i++ {
			if err := connA.Send(protocol.TypeProbe, protocol.ProbePayload{
				Seq:    uint64(i),
				SentAt: int64(i * 1000),
			}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	for i := 0; i < count; i++ {
		msg, err := connB.Recv()
		require.NoError(t, err)
		assert.Equal(t, protocol.TypeProbe, msg.Type)

		var p protocol.ProbePayload
		require.NoError(t, protocol.UnmarshalPayload(msg, &p))
		assert.Equal(t, uint64(i), p.Seq)
	}

	require.NoError(t, <-done)
}

// --- bitfield tests ---

func TestBitfield_SetAndHas(t *testing.T) {
	bf := protocol.NewBitfield(16)

	require.NoError(t, bf.Set(0))
	require.NoError(t, bf.Set(7))
	require.NoError(t, bf.Set(8))
	require.NoError(t, bf.Set(15))

	assert.True(t, bf.Has(0))
	assert.True(t, bf.Has(7))
	assert.True(t, bf.Has(8))
	assert.True(t, bf.Has(15))

	assert.False(t, bf.Has(1))
	assert.False(t, bf.Has(6))
	assert.False(t, bf.Has(9))
	assert.False(t, bf.Has(14))
}

func TestBitfield_OutOfRange(t *testing.T) {
	bf := protocol.NewBitfield(8)
	assert.Error(t, bf.Set(-1))
	assert.Error(t, bf.Set(8))
	assert.False(t, bf.Has(-1))
	assert.False(t, bf.Has(8))
}

func TestBitfield_EncodeDecodeRoundtrip(t *testing.T) {
	bf := protocol.NewBitfield(24)
	require.NoError(t, bf.Set(0))
	require.NoError(t, bf.Set(5))
	require.NoError(t, bf.Set(12))
	require.NoError(t, bf.Set(23))

	encoded := bf.Encode()
	assert.NotEmpty(t, encoded)

	decoded, err := protocol.DecodeBitfield(encoded, 24)
	require.NoError(t, err)

	for i := 0; i < 24; i++ {
		assert.Equal(t, bf.Has(i), decoded.Has(i), "mismatch at index %d", i)
	}
}

func TestBitfield_Missing(t *testing.T) {
	bf := protocol.NewBitfield(5)
	require.NoError(t, bf.Set(1))
	require.NoError(t, bf.Set(3))

	missing := bf.Missing()
	assert.Equal(t, []int{0, 2, 4}, missing)
}

func TestBitfield_Complete(t *testing.T) {
	bf := protocol.NewBitfield(3)
	assert.False(t, bf.Complete())

	require.NoError(t, bf.Set(0))
	require.NoError(t, bf.Set(1))
	assert.False(t, bf.Complete())

	require.NoError(t, bf.Set(2))
	assert.True(t, bf.Complete())
}

func TestBitfield_Count(t *testing.T) {
	bf := protocol.NewBitfield(10)
	assert.Equal(t, 0, bf.Count())

	for i := 0; i < 5; i++ {
		require.NoError(t, bf.Set(i))
	}
	assert.Equal(t, 5, bf.Count())
}

func TestBitfield_DecodeWrongLength(t *testing.T) {
	// 8 pieces needs 1 byte; passing 2-byte hex is wrong
	_, err := protocol.DecodeBitfield("ffff", 8)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "length mismatch")
}

func TestBitfield_DecodeInvalidHex(t *testing.T) {
	_, err := protocol.DecodeBitfield("zzzz", 8)
	require.Error(t, err)
}

func TestBitfield_ZeroPieces(t *testing.T) {
	bf := protocol.NewBitfield(0)
	assert.True(t, bf.Complete(), "zero-piece bitfield is trivially complete")
	assert.Equal(t, 0, bf.Count())
	assert.Empty(t, bf.Missing())
}

func TestBitfield_AllSet(t *testing.T) {
	const n = 13
	bf := protocol.NewBitfield(n)
	for i := 0; i < n; i++ {
		require.NoError(t, bf.Set(i))
	}
	assert.True(t, bf.Complete())
	assert.Empty(t, bf.Missing())
	assert.Equal(t, n, bf.Count())

	// roundtrip
	decoded, err := protocol.DecodeBitfield(bf.Encode(), n)
	require.NoError(t, err)
	assert.True(t, decoded.Complete())
}

// TestBitfield_MSBFirst verifies the bit order matches the wire spec.
// Piece 0 = MSB of byte 0. Setting piece 0 should give byte 0 = 0x80.
func TestBitfield_MSBFirst(t *testing.T) {
	bf := protocol.NewBitfield(8)
	require.NoError(t, bf.Set(0))
	assert.Equal(t, "80", bf.Encode())

	bf2 := protocol.NewBitfield(8)
	require.NoError(t, bf2.Set(7))
	assert.Equal(t, "01", bf2.Encode())
}

// fuzz-style test: encode/decode with all 256 byte patterns
func TestBitfield_ExhaustiveSingleByte(t *testing.T) {
	for b := 0; b < 256; b++ {
		encoded := fmt.Sprintf("%02x", b)
		bf, err := protocol.DecodeBitfield(encoded, 8)
		require.NoError(t, err, "byte %02x", b)

		reencoded := bf.Encode()
		assert.Equal(t, encoded, reencoded, "roundtrip failed for byte %02x", b)
	}
}
