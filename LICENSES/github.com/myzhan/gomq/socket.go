package gomq

import (
	"errors"
	"sync"
	"time"

	"github.com/myzhan/gomq/zmtp"
)

// Socket is the base GoMQ socket type. It should probably
// not be used directly. Specifically typed sockets such
// as ClientSocket, ServerSocket, etc embed this type.
type Socket struct {
	sockType      zmtp.SocketType
	sockID        zmtp.SocketIdentity
	asServer      bool
	conns         map[string]*Connection
	ids           []string
	retryInterval time.Duration
	lock          *sync.RWMutex
	mechanism     zmtp.SecurityMechanism
	recvChannel   chan *zmtp.Message
}

// NewSocket accepts an asServer boolean, zmtp.SocketType, a socket identity and a zmtp.SecurityMechanism
// and returns a *Socket.
func NewSocket(asServer bool, sockType zmtp.SocketType, sockID zmtp.SocketIdentity, mechanism zmtp.SecurityMechanism) *Socket {
	return &Socket{
		lock:          &sync.RWMutex{},
		asServer:      asServer,
		sockType:      sockType,
		sockID:        sockID,
		retryInterval: defaultRetry,
		mechanism:     mechanism,
		conns:         make(map[string]*Connection),
		ids:           make([]string, 0),
		recvChannel:   make(chan *zmtp.Message),
	}
}

// AddConnection adds a gomq.Connection to the socket.
// It is goroutine safe.
func (s *Socket) AddConnection(conn *Connection) {
	s.lock.Lock()
	uuid, err := conn.zmtp.GetIdentity()
	if err != nil || uuid == "" {
		uuid, _ = newUUID()
	}

	s.conns[uuid] = conn
	s.ids = append(s.ids, uuid)
	s.lock.Unlock()
}

// RemoveConnection accepts the uuid of a connection
// and removes that gomq.Connection from the socket
// if it exists. FIXME will bomb if uuid does not
// exist in map
func (s *Socket) RemoveConnection(uuid string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for k, v := range s.ids {
		if v == uuid {
			s.ids = append(s.ids[:k], s.ids[k+1:]...)
			s.conns[uuid].net.Close()
			delete(s.conns, uuid)
		}
	}
}

// GetConnection returns the connection by identity
func (s *Socket) GetConnection(uuid string) (*Connection, error) {
	if conns, ok := s.conns[uuid]; ok {
		return conns, nil
	}
	return nil, errors.New("conn not exist")
}

// RetryInterval returns the retry interval used
// for asyncronous bind / connect.
func (s *Socket) RetryInterval() time.Duration {
	return s.retryInterval
}

// SocketType returns the Socket's zmtp.SocketType.
func (s *Socket) SocketType() zmtp.SocketType {
	return s.sockType
}

// SocketIdentity returns the Socket's zmtp.SocketIdentity.
func (s *Socket) SocketIdentity() zmtp.SocketIdentity {
	return s.sockID
}

// SecurityMechanism returns the Socket's zmtp.SecurityMechanism.
func (s *Socket) SecurityMechanism() zmtp.SecurityMechanism {
	return s.mechanism
}

// RecvChannel returns the Socket's receive channel used
// for receiving messages.
func (s *Socket) RecvChannel() chan *zmtp.Message {
	return s.recvChannel
}

// Close closes all underlying transport connections
// for the socket.
func (s *Socket) Close() {
	s.lock.Lock()
	for k, v := range s.ids {
		s.conns[v].net.Close()
		s.ids = append(s.ids[:k], s.ids[k+1:]...)
	}
	s.lock.Unlock()
}

// Recv receives a message from the Socket's
// message channel and returns it.
func (s *Socket) Recv() ([]byte, error) {
	msg := <-s.recvChannel
	if msg.MessageType == zmtp.CommandMessage {
	}
	return msg.Body[0], msg.Err
}

// Send sends to all conn a message. FIXME should use a channel.
func (s *Socket) Send(b []byte) error {
	for _, conn := range s.conns {
		if err := conn.zmtp.SendFrame(b); err != nil {
			return err
		}
	}
	return nil
}

func (s *Socket) SendMultipart(b [][]byte) error {
	d := make([][]byte, len(b)+1) // FIXME(sbinet): allocates
	d[0] = nil                    // Socket-Identity
	copy(d[1:], b)
	for _, conn := range s.conns {
		if err := conn.zmtp.SendMultipart(d); err != nil {
			return err
		}
	}
	return nil
}

func (s *Socket) RecvMultipart() ([][]byte, error) {
	msg := <-s.recvChannel
	if msg.MessageType == zmtp.CommandMessage {
	}
	return msg.Body, msg.Err
}
