package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
)

const (
	wireVersion    = "1"
	maxMessageSize = 32 * 1024 * 1024 // 32 MB hard cap — PiecePayload with 256 KB data stays well under
)

// Encode writes msgType + payload as a length-prefixed JSON frame to w.
//
// Frame layout:
//
//	[4 bytes big-endian length][JSON(Message)]
func Encode(w io.Writer, msgType MessageType, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode: marshal payload for %s: %w", msgType, err)
	}

	msg := Message{
		Type:    msgType,
		Version: wireVersion,
		Payload: json.RawMessage(raw),
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encode: marshal envelope for %s: %w", msgType, err)
	}

	if len(body) > maxMessageSize {
		return fmt.Errorf("encode: message size %d exceeds limit %d", len(body), maxMessageSize)
	}

	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(body)))

	if _, err := w.Write(prefix[:]); err != nil {
		return fmt.Errorf("encode: write length prefix: %w", err)
	}
	if _, err := w.Write(body); err != nil {
		return fmt.Errorf("encode: write body: %w", err)
	}

	return nil
}

// Decode reads one length-prefixed JSON frame from r and returns the Message.
func Decode(r io.Reader) (*Message, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return nil, fmt.Errorf("decode: read length prefix: %w", err)
	}

	length := binary.BigEndian.Uint32(prefix[:])
	if length == 0 {
		return nil, fmt.Errorf("decode: zero-length message")
	}
	if length > maxMessageSize {
		return nil, fmt.Errorf("decode: message too large: %d bytes (limit %d)", length, maxMessageSize)
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("decode: read body (%d bytes): %w", length, err)
	}

	var msg Message
	if err := json.Unmarshal(buf, &msg); err != nil {
		return nil, fmt.Errorf("decode: unmarshal message: %w", err)
	}

	if msg.Version != wireVersion {
		return nil, fmt.Errorf("decode: unsupported protocol version %q (want %q)", msg.Version, wireVersion)
	}

	return &msg, nil
}

// UnmarshalPayload decodes msg.Payload into dst.
// dst must be a pointer to the expected payload struct.
func UnmarshalPayload(msg *Message, dst any) error {
	if err := json.Unmarshal(msg.Payload, dst); err != nil {
		return fmt.Errorf("unmarshal %s payload: %w", msg.Type, err)
	}
	return nil
}

// Conn wraps a net.Conn with typed Send/Recv methods.
// All methods are safe to call concurrently from a single goroutine;
// callers are responsible for external synchronisation across goroutines.
type Conn struct {
	raw net.Conn
}

// NewConn wraps c. The caller retains ownership of c.
func NewConn(c net.Conn) *Conn {
	return &Conn{raw: c}
}

// Send encodes and writes msgType + payload to the underlying connection.
func (c *Conn) Send(msgType MessageType, payload any) error {
	return Encode(c.raw, msgType, payload)
}

// Recv reads and decodes the next message from the underlying connection.
func (c *Conn) Recv() (*Message, error) {
	return Decode(c.raw)
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	return c.raw.Close()
}

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.raw.RemoteAddr()
}

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr {
	return c.raw.LocalAddr()
}
