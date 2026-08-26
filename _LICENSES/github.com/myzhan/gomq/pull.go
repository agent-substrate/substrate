package gomq

import (
	"net"

	"github.com/myzhan/gomq/zmtp"
)

// PullSocket is a ZMQ_PULL socket type.
// See: http://rfc.zeromq.org/spec:41
type PullSocket struct {
	*Socket
}

// NewPull accepts a zmtp.SecurityMechanism and returns
// a PullSocket as a gomq.Pull interface.
func NewPull(mechanism zmtp.SecurityMechanism) *PullSocket {
	return &PullSocket{
		Socket: NewSocket(false, zmtp.PullSocketType, nil, mechanism),
	}
}

// Bind accepts a zeromq endpoint and binds the
// push socket to it. Currently the only transport
// supported is TCP. The endpoint string should be
// in the format "tcp://<address>:<port>".
func (s *PullSocket) Bind(endpoint string) (net.Addr, error) {
	return BindServer(s, endpoint)
}

// Connect accepts a zeromq endpoint and connects the
// pull socket to it. Currently the only transport
// supported is TCP. The endpoint string should be
// in the format "tcp://<address>:<port>".
func (c *PullSocket) Connect(endpoint string) error {
	return ConnectClient(c, endpoint)
}

var (
	_ Client = (*PullSocket)(nil)
	_ Server = (*PullSocket)(nil)
)
