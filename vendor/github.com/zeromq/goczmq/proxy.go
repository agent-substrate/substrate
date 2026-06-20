package goczmq

/*
#include "czmq.h"

zactor_t *Zproxy_new () {
	zactor_t *proxy = zactor_new(zproxy, NULL);
	return proxy;
}
*/
import "C"

import (
	"unsafe"
)

// Proxy wraps the CZMQ zproxy actor. A proxy actor switches
// messages between a frontend and backend socket, and also
// provides an optional capture socket messages can be
// mirrored to.  The proxy can be paused and resumed.
type Proxy struct {
	zactorT *C.struct__zactor_t
}

// NewProxy creates a new Proxy instance.
func NewProxy() *Proxy {
	p := &Proxy{}
	p.zactorT = C.Zproxy_new()
	return p
}

// SetFrontend accepts a socket type and endpoint, and sends a message
// to the zactor thread telling it to set up a socket bound to the endpoint.
func (p *Proxy) SetFrontend(sockType int, endpoint string) error {
	typeString := getStringType(sockType)

	cmd := C.CString("FRONTEND")
	defer C.free(unsafe.Pointer(cmd))

	cTypeString := C.CString(typeString)
	defer C.free(unsafe.Pointer(cTypeString))

	cEndpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))

	rc := C.zstr_sendm(unsafe.Pointer(p.zactorT), cmd)
	if rc == -1 {
		return ErrActorCmd
	}

	rc = C.zstr_sendm(unsafe.Pointer(p.zactorT), cTypeString)
	if rc == -1 {
		return ErrActorCmd
	}

	rc = C.zstr_send(unsafe.Pointer(p.zactorT), cEndpoint)
	if rc == -1 {
		return ErrActorCmd
	}

	rc = C.zsock_wait(unsafe.Pointer(p.zactorT))
	if rc == -1 {
		return ErrActorCmd
	}

	return nil
}

// SetBackend accepts a socket type and endpoint, and sends a message
// to the zactor thread telling it to set up a socket bound to the endpoint.
func (p *Proxy) SetBackend(sockType int, endpoint string) error {
	typeString := getStringType(sockType)

	cmd := C.CString("BACKEND")
	defer C.free(unsafe.Pointer(cmd))

	cTypeString := C.CString(typeString)
	defer C.free(unsafe.Pointer(cTypeString))

	cEndpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))

	rc := C.zstr_sendm(unsafe.Pointer(p.zactorT), cmd)
	if rc == -1 {
		return ErrActorCmd
	}

	rc = C.zstr_sendm(unsafe.Pointer(p.zactorT), cTypeString)
	if rc == -1 {
		return ErrActorCmd
	}

	rc = C.zstr_send(unsafe.Pointer(p.zactorT), cEndpoint)
	if rc == -1 {
		return ErrActorCmd
	}

	rc = C.zsock_wait(unsafe.Pointer(p.zactorT))
	if rc == -1 {
		return ErrActorCmd
	}

	return nil
}

// SetCapture accepts a socket endpoint and sets up a Push socket bound
// to that endpoint, that sends a copy of all messages passing through
// the proxy.
func (p *Proxy) SetCapture(endpoint string) error {
	cmd := C.CString("CAPTURE")
	defer C.free(unsafe.Pointer(cmd))

	cEndpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))

	rc := C.zstr_sendm(unsafe.Pointer(p.zactorT), cmd)
	if rc == -1 {
		return ErrActorCmd
	}

	rc = C.zstr_send(unsafe.Pointer(p.zactorT), cEndpoint)
	if rc == -1 {
		return ErrActorCmd
	}

	return nil
}

// Pause sends a message to the zproxy actor telling it to pause.
func (p *Proxy) Pause() error {
	cmd := C.CString("PAUSE")
	defer C.free(unsafe.Pointer(cmd))

	rc := C.zstr_send(unsafe.Pointer(p.zactorT), cmd)
	if rc == -1 {
		return ErrActorCmd
	}

	return nil
}

// Resume sends a message to the zproxy actor telling it to resume.
func (p *Proxy) Resume() error {
	cmd := C.CString("RESUME")
	defer C.free(unsafe.Pointer(cmd))

	rc := C.zstr_send(unsafe.Pointer(p.zactorT), cmd)
	if rc == -1 {
		return ErrActorCmd
	}

	return nil
}

// Verbose sets the proxy to log information to stdout.
func (p *Proxy) Verbose() error {
	cmd := C.CString("VERBOSE")
	defer C.free(unsafe.Pointer(cmd))

	rc := C.zstr_send(unsafe.Pointer(p.zactorT), cmd)
	if rc == -1 {
		return ErrActorCmd
	}

	return nil
}

// Destroy destroys the proxy.
func (p *Proxy) Destroy() {
	C.zactor_destroy(&p.zactorT)
}
