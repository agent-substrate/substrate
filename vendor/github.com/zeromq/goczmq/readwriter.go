package goczmq

import (
	"C"
	"io"
)

// ReadWriter provides an io.ReadWriter compatible
// interface for goczmq.Sock
type ReadWriter struct {
	sock          *Sock
	poller        *Poller
	clientIDs     []string
	frame         []byte
	currentIndex  int
	timeoutMillis int
}

// NewReadWriter accepts a sock and returns a goczmq.ReadWriter. The
// io.ReadWriter should now be considered responsible for this
// Sock.
func NewReadWriter(sock *Sock) (*ReadWriter, error) {
	rw := &ReadWriter{
		sock:          sock,
		timeoutMillis: -1,
	}

	var err error
	rw.poller, err = NewPoller(rw.sock)
	return rw, err
}

// SetTimeout sets the timeout on Read in millisecond. If no new
// data is received within the timeout period, Read will return
// an ErrTimeout
func (r *ReadWriter) SetTimeout(ms int) {
	r.timeoutMillis = ms
}

// Read satisifies io.Read
func (r *ReadWriter) Read(p []byte) (int, error) {
	var totalRead int
	var totalFrame int

	var flag int
	var err error

	if r.currentIndex == 0 {
		s := r.poller.Wait(r.timeoutMillis)
		if s == nil {
			return totalRead, ErrTimeout
		}

		r.frame, flag, err = s.RecvFrame()

		if s.GetType() == Router && r.currentIndex == 0 {
			r.clientIDs = append(r.clientIDs, string(r.frame))
			r.frame, flag, err = s.RecvFrame()
		}

		if flag == FlagMore {
			return totalRead, ErrMultiPartUnsupported
		}

		if err != nil {
			return totalRead, io.EOF
		}
	}

	totalRead += copy(p[:], r.frame[r.currentIndex:])
	totalFrame += len(r.frame)

	if totalFrame-r.currentIndex > len(p) {
		r.currentIndex = totalRead
	} else {
		r.currentIndex = 0
		err = io.EOF
	}

	return totalRead, err
}

// Write satisfies io.Write
func (r *ReadWriter) Write(p []byte) (int, error) {
	var total int
	if r.sock.GetType() == Router {
		err := r.sock.SendFrame(r.GetLastClientID(), FlagMore)
		if err != nil {
			return total, err
		}
	}
	err := r.sock.SendFrame(p, 0)
	if err != nil {
		return total, err
	}

	return len(p), nil

}

// GetLastClientID returns the id of the last client you received
// a message from if the underlying socket is a Router socket
func (r *ReadWriter) GetLastClientID() []byte {
	id := []byte(r.clientIDs[0])
	r.clientIDs = r.clientIDs[1:]
	return id
}

// SetLastClientID lets you manually set the id of the client
// you last received a message from if the underlying socket
// is a Router socket
func (r *ReadWriter) SetLastClientID(id []byte) {
	r.clientIDs = append(r.clientIDs, string(id))
}

// Destroy destroys both the ReadWriter and the underlying Sock
func (r *ReadWriter) Destroy() {
	r.sock.Destroy()
	r.poller.Destroy()
}
