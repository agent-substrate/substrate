package gomq

import "github.com/myzhan/gomq/zmtp"

// DealerSocket is a ZMQ_DEALER socket type.
// See: https://rfc.zeromq.org/spec:28
type DealerSocket struct {
	*Socket
}

// NewDealer accepts a zmtp.SecurityMechanism and an ID.
// It returns a DealerSocket as a gomq.Dealer interface.
func NewDealer(mechanism zmtp.SecurityMechanism, id string) Dealer {
	return &DealerSocket{
		Socket: NewSocket(false, zmtp.DealerSocketType, zmtp.SocketIdentity(id), mechanism),
	}
}

// Connect accepts a zeromq endpoint and connects the
// dealer socket to it. Currently the only transport
// supported is TCP. The endpoint string should be
// in the format "tcp://<address>:<port>".
func (d *DealerSocket) Connect(endpoint string) error {
	return ConnectDealer(d, endpoint)
}

var (
	_ Dealer = (*DealerSocket)(nil)
)
