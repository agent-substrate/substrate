// +build draft

package goczmq

/*
#include "czmq.h"
*/
import "C"

const (
	// Scatter is a ZMQ_SCATTER socket type
	Scatter = int(C.ZMQ_SCATTER)

	// Gather is a ZMQ_GATHER socket type
	Gather = int(C.ZMQ_GATHER)
)

// NewGather creates a Gather socket and calls Attach.
// The socket will Bind by default.
func NewGather(endpoints string) (*Sock, error) {
	s := NewSock(Gather)
	return s, s.Attach(endpoints, true)
}

// NewScatter creates a Scatter socket and calls Attach.
// The socket will Connect by default.
func NewScatter(endpoints string) (*Sock, error) {
	s := NewSock(Scatter)
	return s, s.Attach(endpoints, false)
}
