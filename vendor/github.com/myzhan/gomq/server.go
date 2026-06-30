package gomq

import (
	"net"

	"github.com/myzhan/gomq/zmtp"
)

// ServerSocket is a ZMQ_SERVER socket type.
// See: http://rfc.zeromq.org/spec:41
type ServerSocket struct {
	*Socket
}

// NewServer accepts a zmtp.SecurityMechanism and returns
// a ServerSocket as a gomq.Server interface.
func NewServer(mechanism zmtp.SecurityMechanism) Server {
	return &ServerSocket{
		Socket: NewSocket(true, zmtp.ServerSocketType, nil, mechanism),
	}
}

// Bind accepts a zeromq endpoint and binds the
// server socket to it. Currently the only transport
// supported is TCP. The endpoint string should be
// in the format "tcp://<address>:<port>".
func (s *ServerSocket) Bind(endpoint string) (net.Addr, error) {
	return BindServer(s, endpoint)
}
