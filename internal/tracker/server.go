package tracker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync/atomic"

	"github.com/manoj-1407/swarmfs/internal/protocol"
)

// Server is the SwarmFS tracker.
// Each incoming TCP connection carries exactly one request message and
// receives zero or one response, then closes.
type Server struct {
	addr      string
	registry  *Registry
	boundAddr atomic.Value // stores string; written once in Serve, read by ListenAddr
}

// NewServer returns a Server that will listen on addr.
func NewServer(addr string) *Server {
	return &Server{
		addr:     addr,
		registry: NewRegistry(),
	}
}

// Registry exposes the underlying registry for testing.
func (s *Server) Registry() *Registry { return s.registry }

// ListenAddr returns the address the server is actually bound to.
// Returns "" until Serve has bound its listener.
func (s *Server) ListenAddr() string {
	if v := s.boundAddr.Load(); v != nil {
		return v.(string)
	}
	return ""
}

// Serve starts accepting connections. It blocks until ctx is cancelled or a
// fatal listen error occurs.
func (s *Server) Serve(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("tracker listen on %s: %w", s.addr, err)
	}
	// publish the real address before accepting so ListenAddr() is race-free
	s.boundAddr.Store(ln.Addr().String())

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil // normal shutdown
			}
			log.Printf("tracker: accept error: %v", err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(raw net.Conn) {
	defer raw.Close()
	c := protocol.NewConn(raw)

	msg, err := c.Recv()
	if err != nil {
		return
	}

	switch msg.Type {
	case protocol.TypeRegister:
		s.handleRegister(c, msg)
	case protocol.TypePeerListReq:
		s.handlePeerListReq(c, msg)
	case protocol.TypeHave:
		s.handleHave(c, msg)
	case protocol.TypeHeartbeat:
		s.handleHeartbeat(c, msg)
	case protocol.TypeLeave:
		s.handleLeave(c, msg)
	default:
		_ = c.Send(protocol.TypeAck, protocol.AckPayload{
			OK:      false,
			Message: fmt.Sprintf("unknown message type: %s", msg.Type),
		})
	}
}

func (s *Server) handleRegister(c *protocol.Conn, msg *protocol.Message) {
	var p protocol.RegisterPayload
	if err := protocol.UnmarshalPayload(msg, &p); err != nil {
		ack(c, false, "bad register payload")
		return
	}
	if p.FileHash == "" || p.PeerAddr == "" {
		ack(c, false, "fileHash and peerAddr are required")
		return
	}
	s.registry.Register(p.FileHash, p.PeerAddr, p.Bitfield)
	ack(c, true, "")
}

func (s *Server) handlePeerListReq(c *protocol.Conn, msg *protocol.Message) {
	var p protocol.PeerListReqPayload
	if err := protocol.UnmarshalPayload(msg, &p); err != nil {
		ack(c, false, "bad peer_list_req payload")
		return
	}
	peers := s.registry.Peers(p.FileHash, p.RequesterAddr)
	_ = c.Send(protocol.TypePeerList, protocol.PeerListPayload{Peers: peers})
}

func (s *Server) handleHave(c *protocol.Conn, msg *protocol.Message) {
	var p protocol.HavePayload
	if err := protocol.UnmarshalPayload(msg, &p); err != nil {
		ack(c, false, "bad have payload")
		return
	}
	s.registry.UpdateHave(p.FileHash, p.PeerAddr)
	ack(c, true, "")
}

func (s *Server) handleHeartbeat(c *protocol.Conn, msg *protocol.Message) {
	var p protocol.HeartbeatPayload
	if err := protocol.UnmarshalPayload(msg, &p); err != nil {
		ack(c, false, "bad heartbeat payload")
		return
	}
	ack(c, true, "")
}

func (s *Server) handleLeave(c *protocol.Conn, msg *protocol.Message) {
	var p protocol.LeavePayload
	if err := protocol.UnmarshalPayload(msg, &p); err != nil {
		ack(c, false, "bad leave payload")
		return
	}
	s.registry.Remove(p.FileHash, p.PeerAddr)
	ack(c, true, "")
}

func ack(c *protocol.Conn, ok bool, message string) {
	_ = c.Send(protocol.TypeAck, protocol.AckPayload{OK: ok, Message: message})
}

func trackerCall(trackerAddr string, msgType protocol.MessageType, payload any, readResponse bool) (*protocol.Message, error) {
	conn, err := net.Dial("tcp", trackerAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to tracker %s: %w", trackerAddr, err)
	}
	defer conn.Close()

	c := protocol.NewConn(conn)
	if err := c.Send(msgType, payload); err != nil {
		return nil, fmt.Errorf("send %s to tracker: %w", msgType, err)
	}
	if !readResponse {
		return nil, nil
	}

	msg, err := c.Recv()
	if err != nil {
		return nil, fmt.Errorf("recv from tracker after %s: %w", msgType, err)
	}
	return msg, nil
}

// Register sends a REGISTER message to trackerAddr and waits for ACK.
func Register(trackerAddr, fileHash, peerAddr, bitfield string) error {
	msg, err := trackerCall(trackerAddr, protocol.TypeRegister, protocol.RegisterPayload{
		FileHash: fileHash,
		PeerAddr: peerAddr,
		Bitfield: bitfield,
	}, true)
	if err != nil {
		return err
	}
	return expectACK(msg)
}

// GetPeers sends a PEER_LIST_REQ and returns the peer list.
func GetPeers(trackerAddr, fileHash, requesterID string) ([]protocol.PeerInfo, error) {
	msg, err := trackerCall(trackerAddr, protocol.TypePeerListReq, protocol.PeerListReqPayload{
		FileHash:      fileHash,
		RequesterAddr: requesterID,
	}, true)
	if err != nil {
		return nil, err
	}
	if msg.Type != protocol.TypePeerList {
		return nil, fmt.Errorf("expected PEER_LIST, got %s", msg.Type)
	}
	var p protocol.PeerListPayload
	if err := protocol.UnmarshalPayload(msg, &p); err != nil {
		return nil, err
	}
	return p.Peers, nil
}

// AnnounceHave sends a HAVE message to the tracker.
func AnnounceHave(trackerAddr, fileHash, peerAddr string, pieceIndex int) error {
	msg, err := trackerCall(trackerAddr, protocol.TypeHave, protocol.HavePayload{
		FileHash:   fileHash,
		PieceIndex: pieceIndex,
		PeerAddr:   peerAddr,
	}, true)
	if err != nil {
		return err
	}
	return expectACK(msg)
}

// Leave removes the peer from the tracker.
func Leave(trackerAddr, fileHash, peerAddr string) error {
	msg, err := trackerCall(trackerAddr, protocol.TypeLeave, protocol.LeavePayload{
		FileHash: fileHash,
		PeerAddr: peerAddr,
	}, true)
	if err != nil {
		return err
	}
	return expectACK(msg)
}

func expectACK(msg *protocol.Message) error {
	if msg.Type != protocol.TypeAck {
		return fmt.Errorf("expected ACK, got %s", msg.Type)
	}
	var a protocol.AckPayload
	if err := protocol.UnmarshalPayload(msg, &a); err != nil {
		return err
	}
	if !a.OK {
		return errors.New(a.Message)
	}
	return nil
}
