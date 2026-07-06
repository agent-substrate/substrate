package gomq

import "github.com/myzhan/gomq/zmtp"

// ClientSocket is a ZMQ_CLIENT socket type.
// See: http://rfc.zeromq.org/spec:41
type ClientSocket struct {
	*Socket
}

// NewClient accepts a zmtp.SecurityMechanism and returns
// a ClientSocket as a gomq.Client interface.
func NewClient(mechanism zmtp.SecurityMechanism) Client {
	return &ClientSocket{
		Socket: NewSocket(false, zmtp.ClientSocketType, nil, mechanism),
	}
}

// Connect accepts a zeromq endpoint and connects the
// client socket to it. Currently the only transport
// supported is TCP. The endpoint string should be
// in the format "tcp://<address>:<port>".
func (c *ClientSocket) Connect(endpoint string) error {
	return ConnectClient(c, endpoint)
}
