// Package goczmq is a golang binding for CZMQ 3. CZMQ is a high level binding
// for ZeroMQ. Along with ZeroMQ socket support, CZMQ provides "actor" based
// services for authentication, service discovery, and creating proxies.
// GoCZMQ provides direct bindings to CZMQ along with higher level go
// abstractions such as channels and io.ReadWriter interface support.
//
// "Tell them I was a writer.
//  A maker of software.
//  A humanist. A father.
//  And many things.
//  But above all, a writer.
//  Thank You. :)
//  - Pieter Hintjens
package goczmq

/*
#cgo !windows pkg-config: libczmq libzmq libsodium
#cgo windows LDFLAGS: -lws2_32 -liphlpapi -lrpcrt4 -lsodium -lzmq -lczmq
#cgo windows CFLAGS: -Wno-pedantic-ms-format -DLIBCZMQ_EXPORTS -DZMQ_DEFINED_STDINT -DLIBCZMQ_EXPORTS -DZMQ_BUILD_DRAFT_API

#include "czmq.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"errors"
)

const (
	// Req is a ZMQ_REQ socket type
	Req = int(C.ZMQ_REQ)

	// Rep is a ZMQ_REP socket type
	Rep = int(C.ZMQ_REP)

	// Dealer is a ZMQ_DEALER socket type
	Dealer = int(C.ZMQ_DEALER)

	// Router is a ZMQ_ROUTER socket type
	Router = int(C.ZMQ_ROUTER)

	// Pub is a ZMQ_PUB socket type
	Pub = int(C.ZMQ_PUB)

	// Sub is a ZMQ_SUB socket type
	Sub = int(C.ZMQ_SUB)

	// XPub is a ZMQ_XPUB socket type
	XPub = int(C.ZMQ_XPUB)

	// XSub is a ZMQ_XSUB socket type
	XSub = int(C.ZMQ_XSUB)

	// Push is a ZMQ_PUSH socket type
	Push = int(C.ZMQ_PUSH)

	// Pull is a ZMQ_PULL socket type
	Pull = int(C.ZMQ_PULL)

	// Pair is a ZMQ_PAIR socket type
	Pair = int(C.ZMQ_PAIR)

	// Stream is a ZMQ_STREAM socket type
	Stream = int(C.ZMQ_STREAM)

	// Pollin is the ZMQ_POLLIN constant
	Pollin = int(C.ZMQ_POLLIN)

	// Pollout is the ZMQ_POLLOUT constant
	Pollout = int(C.ZMQ_POLLOUT)

	// FlagMore is the ZFRAME_MORE flag
	FlagMore = int(C.ZFRAME_MORE)

	// FlagReuse is the ZFRAME_REUSE flag
	FlagReuse = int(C.ZFRAME_REUSE)

	//FlagDontWait is the ZFRAME_DONTWAIT flag
	FlagDontWait = int(C.ZFRAME_DONTWAIT)

	//FlagNone means there are no flags
	FlagNone = 0

	// CurveAllowAny is a semantic convenience for allowing
	// any Curve clients
	CurveAllowAny = "*"

	//ZMQVersionMajor is the major version of the underlying ZeroMQ library
	ZMQVersionMajor = int(C.ZMQ_VERSION_MAJOR)

	//ZMQVersionMinor is the minor version of the underlying ZeroMQ library
	ZMQVersionMinor = int(C.ZMQ_VERSION_MINOR)

	//CZMQVersionMajor is the major version of the underlying CZMQ library
	CZMQVersionMajor = int(C.CZMQ_VERSION_MAJOR)

	// CZMQVersionMinor is the minor version of the underlying CZMQ library
	CZMQVersionMinor = int(C.CZMQ_VERSION_MINOR)
)

var (
	// ErrActorCmd is returned when there is an error sending
	// a command to an actor
	ErrActorCmd = errors.New("error sending actor command")

	// ErrSockAttach is returned when an attach call to a socket fails
	ErrSockAttach = errors.New("error attaching zsock")

	// ErrInvalidSockType is returned when a function is called
	// against a socket type that is not applicable for that socket type
	ErrInvalidSockType = errors.New("invalid socket type")

	// ErrSliceFull is returned if a []byte passed to Read was not
	// large enough to hold the contents of a message
	ErrSliceFull = errors.New("slice full")

	// ErrConnect is returned if Connect on a socket fails
	ErrConnect = errors.New("connect error")

	// ErrDisconnect is returned if Disconnect on a socket fails
	ErrDisconnect = errors.New("disconnect error")

	// ErrBind is returned if Bind on a socket fails
	ErrBind = errors.New("bind error")

	// ErrUnbind is returned if Unbind on a socket fails
	ErrUnbind = errors.New("unbind error")

	// ErrSendFrame is returned if SendFrame on a socket fails
	ErrSendFrame = errors.New("send frame error")

	// ErrRecvFrame is returned if RecvFrame on a socket fails
	ErrRecvFrame = errors.New("recv frame error")

	// ErrRecvFrameAfterDestroy is returned if RecvFrame is called
	// on a socket after it has been destroyed.
	ErrRecvFrameAfterDestroy = errors.New("RecvFrame() is invalid on socket after Detroy() has been called.")

	// ErrRecvMessage is returned if RecvMessage on a socket fails
	ErrRecvMessage = errors.New("recv message error")

	// ErrWaitAfterDestroy is returned by a Poller if there is an error
	// accessing the underlying socket pointer when Wait is called
	ErrWaitAfterDestroy = errors.New("Wait() is invalid on Poller after Destroy() is called.")

	// ErrMultiPartUnsupported is returned when a function that does
	// not support multi-part messages encounters a multi-part message
	ErrMultiPartUnsupported = errors.New("function does not support multi part messages")

	// ErrTimeout is returned when a function that supports timeouts times out
	ErrTimeout = errors.New("function timed out")

	// ErrCertNotFound is returned when NewCertFromFile tries to
	// load a file that does not exist.
	ErrCertNotFound = errors.New("file not found")
)

// Shutdown shuts down the CZMQ zsys layer.
// The CZMQ zsys layer normally shuts down on process termination through the
// use of an atexit cleanup function. Calling this allows the zsys layer to be
// shutdown manually.
//
// This is beneficial when CZMQ will no longer be used but the process will not
// be terminating. Any potential resources allocated by the zsys layer can be
// freed as they will no longer be needed.
func Shutdown() {
	C.zsys_shutdown()
}

func getStringType(k int) string {
	switch k {
	case Req:
		return "REQ"
	case Rep:
		return "REP"
	case Dealer:
		return "DEALER"
	case Router:
		return "ROUTER"
	case Pub:
		return "PUB"
	case Sub:
		return "SUB"
	case XPub:
		return "XPUB"
	case XSub:
		return "XSUB"
	case Push:
		return "PUSH"
	case Pull:
		return "PULL"
	case Pair:
		return "PAIR"
	case Stream:
		return "STREAM"
	default:
		return ""
	}
}
