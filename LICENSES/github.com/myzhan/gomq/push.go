package gomq

import (
	"net"

	"github.com/myzhan/gomq/zmtp"
)

// PushSocket is a ZMQ_PUSH socket type.
// See: http://rfc.zeromq.org/spec:41
type PushSocket struct {
	*Socket
}

// NewPush accepts a zmtp.SecurityMechanism and returns
// a PushSocket as a gomq.Push interface.
func NewPush(mechanism zmtp.SecurityMechanism) *PushSocket {
	return &PushSocket{
		Socket: NewSocket(false, zmtp.PushSocketType, nil, mechanism),
	}
}

// Bind accepts a zeromq endpoint and binds the
// push socket to it. Currently the only transport
// supported is TCP. The endpoint string should be
// in the format "tcp://<address>:<port>".
func (s *PushSocket) Bind(endpoint string) (net.Addr, error) {
	return BindServer(s, endpoint)
}

// Connect accepts a zeromq endpoint and connects the
// client socket to it. Currently the only transport
// supported is TCP. The endpoint string should be
// in the format "tcp://<address>:<port>".
func (s *PushSocket) Connect(endpoint string) error {
	return ConnectClient(s, endpoint)
}

var (
	_ Client = (*PushSocket)(nil)
	_ Server = (*PushSocket)(nil)
)
