package gomq

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/myzhan/gomq/zmtp"
)

type ErrBadProto string

func (e ErrBadProto) Error() string {
	return fmt.Sprintf("Protocol '%s' not known to gomq", string(e))
}

var (
	defaultRetry = 250 * time.Millisecond
)

// Connection is a gomq connection. It holds
// both the net.Conn transport as well as the
// zmtp connection information.
type Connection struct {
	net  net.Conn
	zmtp *zmtp.Connection
}

// NewConnection accepts a net.Conn, a *zmtp.Connection
// and returns a *gomq.Connection.
func NewConnection(netConn net.Conn, zmtpConn *zmtp.Connection) *Connection {
	conn := &Connection{
		net:  netConn,
		zmtp: zmtpConn,
	}
	return conn
}

// Send sends a message. FIXME should use a channel.
func (c *Connection) Send(b []byte) error {
	return c.zmtp.SendFrame(b)
}

// SendMultipart ...
func (c *Connection) SendMultipart(b [][]byte) error {
	d := make([][]byte, len(b)+1) // FIXME(sbinet): allocates
	d[0] = nil                    // Socket-Identity
	copy(d[1:], b)
	return c.zmtp.SendMultipart(d)
}

// ZeroMQSocket is the base gomq interface.
type ZeroMQSocket interface {
	Recv() ([]byte, error)
	Send([]byte) error
	RetryInterval() time.Duration
	SocketType() zmtp.SocketType
	SocketIdentity() zmtp.SocketIdentity
	SecurityMechanism() zmtp.SecurityMechanism
	AddConnection(*Connection)
	RemoveConnection(string)
	RecvChannel() chan *zmtp.Message

	SendMultipart([][]byte) error
	RecvMultipart() ([][]byte, error)

	Close()
}

// Client is a gomq interface used for client sockets.
// It implements the Socket interface along with a
// Connect method for connecting to endpoints.
type Client interface {
	ZeroMQSocket
	Connect(endpoint string) error
}

// ConnectClient accepts a Client interface and an endpoint
// in the format <proto>://<address>:<port>. It then attempts
// to connect to the endpoint and perform a ZMTP handshake.
func ConnectClient(c Client, endpoint string) error {
	parts := strings.Split(endpoint, "://")

	if parts[0] != "tcp" {
		return ErrBadProto(parts[0])
	}

Connect:
	netConn, err := net.Dial(parts[0], parts[1])
	if err != nil {
		time.Sleep(c.RetryInterval())
		goto Connect
	}

	zmtpConn := zmtp.NewConnection(netConn)
	_, err = zmtpConn.Prepare(c.SecurityMechanism(), c.SocketType(), c.SocketIdentity(), false, nil)
	if err != nil {
		return err
	}

	conn := &Connection{
		net:  netConn,
		zmtp: zmtpConn,
	}

	c.AddConnection(conn)
	zmtpConn.Recv(c.RecvChannel())
	return nil
}

// Server is a gomq interface used for server sockets.
// It implements the Socket interface along with a
// Bind method for binding to endpoints.
type Server interface {
	ZeroMQSocket
	Bind(endpoint string) (net.Addr, error)
}

// BindServer accepts a Server interface and an endpoint
// in the format <proto>://<address>:<port>. It then attempts
// to bind to the endpoint.
func BindServer(s Server, endpoint string) (net.Addr, error) {
	var addr net.Addr
	parts := strings.Split(endpoint, "://")

	ln, err := net.Listen(parts[0], parts[1])
	if err != nil {
		return addr, err
	}

	go func() {
		for {
			netConn, err := ln.Accept()
			if err != nil {
				continue
			}

			go func() {
				zmtpConn := zmtp.NewConnection(netConn)
				_, err = zmtpConn.Prepare(s.SecurityMechanism(), s.SocketType(), s.SocketIdentity(), true, nil)

				if err != nil {
					return
				}

				// replace conn, if identity already exist.
				identity, _ := zmtpConn.GetIdentity()
				s.RemoveConnection(identity)
				s.AddConnection(NewConnection(netConn, zmtpConn))

				// remove conn, if the connection has lost.
				zmtpMsgs := make(chan *zmtp.Message, 3)
				zmtpConn.Recv(zmtpMsgs)
				for {
					msg := <-zmtpMsgs

					if msg != nil && msg.Err == io.EOF {
						s.RemoveConnection(identity)
						break
					}

					s.RecvChannel() <- msg
				}

			}()
		}
	}()
	time.Sleep(500 * time.Millisecond)
	return ln.Addr(), nil
}

// Dealer is a gomq interface used for dealer sockets.
// It implements the Socket interface along with a Connect method for
// connecting to endpoints.
type Dealer interface {
	ZeroMQSocket
	Connect(endpoint string) error
}

// ConnectDealer accepts a Dealer interface and an endpoint
// in the format <proto>://<address>:<port>. It then attempts
// to connect to the endpoint and perform a ZMTP handshake.
func ConnectDealer(d Dealer, endpoint string) error {
	parts := strings.Split(endpoint, "://")

Connect:
	netConn, err := net.Dial(parts[0], parts[1])
	if err != nil {
		time.Sleep(d.RetryInterval())
		goto Connect
	}

	zmtpConn := zmtp.NewConnection(netConn)
	_, err = zmtpConn.Prepare(d.SecurityMechanism(), d.SocketType(), d.SocketIdentity(), false, nil)
	if err != nil {
		return err
	}

	conn := &Connection{
		net:  netConn,
		zmtp: zmtpConn,
	}

	d.AddConnection(conn)
	zmtpConn.RecvMultipart(d.RecvChannel())
	return nil
}
