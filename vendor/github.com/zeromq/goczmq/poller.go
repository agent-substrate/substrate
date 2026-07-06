package goczmq

/*
#include "czmq.h"

zpoller_t *Poller_new(void *reader) {
	zpoller_t *poller = zpoller_new(reader, NULL);
	return poller;
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// Poller provides a simple wrapper to ZeroMQ's zmq_poll API,
// for the common case of reading from a number of sockets.
// Sockets can be added and removed from the running poller.
type Poller struct {
	zpollerT *C.struct__zpoller_t
	socks    []*Sock
}

// NewPoller creates a new Poller instance.
// It accepts one or more readers to poll.
func NewPoller(readers ...*Sock) (*Poller, error) {
	var p *Poller
	if len(readers) == 0 {
		p = &Poller{
			zpollerT: C.Poller_new(nil),
			socks:    make([]*Sock, 0),
		}
	} else {
		p = &Poller{
			zpollerT: C.Poller_new(unsafe.Pointer(readers[0].zsockT)),
			socks:    make([]*Sock, 0),
		}

		p.socks = append(p.socks, readers[0])
		if len(readers) == 1 {
			return p, nil
		}

		for _, reader := range readers[1:] {
			err := p.Add(reader)
			if err != nil {
				return nil, err
			}
		}
	}
	return p, nil
}

// Add adds a reader to be polled.
func (p *Poller) Add(reader *Sock) error {
	rc := C.zpoller_add(p.zpollerT, unsafe.Pointer(reader.zsockT))
	if int(rc) == -1 {
		return fmt.Errorf("error adding reader")
	}
	p.socks = append(p.socks, reader)
	return nil
}

// Remove removes a Sock from the poller
func (p *Poller) Remove(reader *Sock) {
	numItems := len(p.socks)
	for i := 0; i < numItems; i++ {
		if p.socks[i] == reader {
			if i == numItems-1 {
				p.socks = p.socks[:i]
			} else {
				p.socks = append(p.socks[:i], p.socks[i+1:]...)
			}
		}
	}
}

// Wait waits for the timeout period in milliseconds for a Pollin
// event, and returns the first socket that returns one
func (p *Poller) Wait(millis int) *Sock {
	if p.zpollerT == nil {
		// Null pointer. Something is wrong or we've already had `Destroy` invoked on us.
		panic(ErrWaitAfterDestroy)
	}
	s := C.zpoller_wait(p.zpollerT, C.int(millis))
	s = unsafe.Pointer(s)
	if s == nil {
		return nil
	}
	for _, sock := range p.socks {
		if unsafe.Pointer(sock.zsockT) == s {
			return sock
		}
	}

	panic(fmt.Sprintf(
		"Could not match received pointer with %v with any socket (%v)",
		s, p.socks))
}

// Destroy destroys the Poller
func (p *Poller) Destroy() {
	C.zpoller_destroy(&p.zpollerT)
}
