// Package protocol defines the SwarmFS wire format.
// Every message is a length-prefixed JSON frame: [4-byte big-endian length][JSON].
package protocol

import "encoding/json"

// MessageType identifies the purpose of a message.
type MessageType string

const (
	// tracker ↔ peer
	TypeRegister    MessageType = "REGISTER"
	TypePeerListReq MessageType = "PEER_LIST_REQ"
	TypePeerList    MessageType = "PEER_LIST"
	TypeHave        MessageType = "HAVE"
	TypeHeartbeat   MessageType = "HEARTBEAT"
	TypeLeave       MessageType = "LEAVE"
	TypeAck         MessageType = "ACK"

	// peer ↔ peer
	TypeHandshake      MessageType = "HANDSHAKE"
	TypeInterested     MessageType = "INTERESTED"
	TypeNotInterested  MessageType = "NOT_INTERESTED"
	TypeChoke          MessageType = "CHOKE"
	TypeUnchoke        MessageType = "UNCHOKE"
	TypeRequest        MessageType = "REQUEST"
	TypePiece          MessageType = "PIECE"
	TypeProbe          MessageType = "PROBE"
	TypeProbeAck       MessageType = "PROBE_ACK"
	TypeBitfieldUpdate MessageType = "BITFIELD_UPDATE"
	TypeManifestReq    MessageType = "MANIFEST_REQ"
	TypeManifest       MessageType = "MANIFEST"
)

// Message is the top-level wire envelope.
type Message struct {
	Type    MessageType     `json:"type"`
	Version string          `json:"v"`
	Payload json.RawMessage `json:"p"`
}

// --- tracker ↔ peer payloads ---

// RegisterPayload is sent by a peer to announce itself and its current pieces.
type RegisterPayload struct {
	FileHash string `json:"fileHash"`
	PeerAddr string `json:"peerAddr"`
	Bitfield string `json:"bitfield"` // hex-encoded, see protocol.Bitfield
}

// AckPayload is a generic acknowledgement for tracker commands.
type AckPayload struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

// PeerListReqPayload requests the set of peers that have pieces of a file.
// RequesterAddr is excluded from the result so a peer doesn't get itself back.
type PeerListReqPayload struct {
	FileHash      string `json:"fileHash"`
	RequesterAddr string `json:"requesterAddr,omitempty"`
}

// ManifestReqPayload asks a seeder to send the manifest for fileHash.
type ManifestReqPayload struct {
	FileHash string `json:"fileHash"`
}

// ManifestPayload carries a JSON-encoded piece.Manifest from seeder to leecher.
type ManifestPayload struct {
	FileHash string `json:"fileHash"`
	Raw      []byte `json:"raw"` // JSON-encoded piece.Manifest
}

// PeerInfo describes a single peer in a peer list response.
type PeerInfo struct {
	Addr     string `json:"addr"`
	Bitfield string `json:"bitfield"`
}

// PeerListPayload is the tracker's response to PeerListReq.
type PeerListPayload struct {
	Peers []PeerInfo `json:"peers"`
}

// HavePayload is sent when a peer finishes downloading a piece.
// Direction: peer → tracker and peer → peers (broadcast).
type HavePayload struct {
	FileHash   string `json:"fileHash"`
	PieceIndex int    `json:"pieceIndex"`
	PeerAddr   string `json:"peerAddr"`
}

// HeartbeatPayload keeps the tracker registration alive.
type HeartbeatPayload struct {
	PeerAddr string `json:"peerAddr"`
}

// LeavePayload is sent when a peer gracefully disconnects.
type LeavePayload struct {
	PeerAddr string `json:"peerAddr"`
	FileHash string `json:"fileHash"`
}

// --- peer ↔ peer payloads ---

// HandshakePayload is the first message sent on a peer connection.
type HandshakePayload struct {
	PeerID   string `json:"peerID"`
	FileHash string `json:"fileHash"`
	Bitfield string `json:"bitfield"`
}

// RequestPayload asks the remote peer to send a specific piece.
type RequestPayload struct {
	PieceIndex int `json:"pieceIndex"`
}

// PiecePayload carries raw piece bytes and its hash for verification.
type PiecePayload struct {
	PieceIndex int    `json:"pieceIndex"`
	Data       []byte `json:"data"`   // raw bytes
	Hash       string `json:"hash"`   // sha256 hex of Data
}

// ProbePayload is sent to measure RTT. The receiver responds with ProbeAck.
type ProbePayload struct {
	Seq    uint64 `json:"seq"`
	SentAt int64  `json:"sentAt"` // unix nanoseconds
}

// ProbeAckPayload echoes the probe and adds the receipt timestamp.
type ProbeAckPayload struct {
	Seq     uint64 `json:"seq"`
	SentAt  int64  `json:"sentAt"`  // echoed from ProbePayload
	AckedAt int64  `json:"ackedAt"` // unix nanoseconds, when probe arrived
}

// BitfieldUpdatePayload is broadcast when a peer acquires a new piece.
// Receivers update their local availability map.
type BitfieldUpdatePayload struct {
	Bitfield string `json:"bitfield"`
}

// empty payloads for control messages that carry no data
type emptyPayload struct{}

// EmptyPayload is used for INTERESTED, NOT_INTERESTED, CHOKE, UNCHOKE.
var EmptyPayload = emptyPayload{}
