package goczmq

/*
#include "czmq.h"

zactor_t *Auth_new () {
	zactor_t *auth = zactor_new(zauth, NULL); return auth;
}
*/
import "C"

import (
	"unsafe"
)

// Auth wraps the CZMQ zauth actor. It handles authentication
// for all incoming connections. It allows whitelisting and
// blackisting peers based on IP address and support
// PLAIN and CURVE authentication policies.
type Auth struct {
	zactorT *C.struct__zactor_t
}

// NewAuth creates a new Auth actor.
func NewAuth() *Auth {
	z := &Auth{}
	z.zactorT = C.Auth_new()
	return z
}

// Verbose sets the auth actor to log information to stdout.
func (a *Auth) Verbose() error {
	cmd := C.CString("VERBOSE")
	defer C.free(unsafe.Pointer(cmd))

	rc := C.zstr_send(unsafe.Pointer(a.zactorT), cmd)
	if rc == -1 {
		return ErrActorCmd
	}
	C.zsock_wait(unsafe.Pointer(a.zactorT))

	return nil
}

// Deny adds an address to a socket's deny list
func (a *Auth) Deny(address string) error {
	cmd := C.CString("DENY")
	defer C.free(unsafe.Pointer(cmd))

	cAddress := C.CString(address)
	defer C.free(unsafe.Pointer(cAddress))

	rc := C.zstr_sendm(unsafe.Pointer(a.zactorT), cmd)
	if rc == -1 {
		return ErrActorCmd
	}

	rc = C.zstr_send(unsafe.Pointer(a.zactorT), cAddress)
	if rc == -1 {
		return ErrActorCmd
	}

	C.zsock_wait(unsafe.Pointer(a.zactorT))

	return nil
}

// Allow removes a previous Deny
func (a *Auth) Allow(address string) error {
	cmd := C.CString("ALLOW")
	defer C.free(unsafe.Pointer(cmd))

	cAddress := C.CString(address)
	defer C.free(unsafe.Pointer(cAddress))

	rc := C.zstr_sendm(unsafe.Pointer(a.zactorT), cmd)
	if rc == -1 {
		return ErrActorCmd
	}

	rc = C.zstr_send(unsafe.Pointer(a.zactorT), cAddress)
	if rc == -1 {
		return ErrActorCmd
	}

	C.zsock_wait(unsafe.Pointer(a.zactorT))

	return nil
}

// Curve sets auth method to curve
func (a *Auth) Curve(allowed string) error {
	cmd := C.CString("CURVE")
	defer C.free(unsafe.Pointer(cmd))

	cAllowed := C.CString(allowed)
	defer C.free(unsafe.Pointer(cAllowed))

	rc := C.zstr_sendm(unsafe.Pointer(a.zactorT), cmd)
	if rc == -1 {
		return ErrActorCmd
	}

	rc = C.zstr_send(unsafe.Pointer(a.zactorT), cAllowed)
	if rc == -1 {
		return ErrActorCmd
	}

	C.zsock_wait(unsafe.Pointer(a.zactorT))

	return nil
}

// Plain sets auth method to plain
func (a *Auth) Plain(directory string) error {
	cmd := C.CString("PLAIN")
	defer C.free(unsafe.Pointer(cmd))

	cDirectory := C.CString(directory)
	defer C.free(unsafe.Pointer(cDirectory))

	rc := C.zstr_sendm(unsafe.Pointer(a.zactorT), cmd)
	if rc == -1 {
		return ErrActorCmd
	}

	rc = C.zstr_send(unsafe.Pointer(a.zactorT), cDirectory)
	if rc == -1 {
		return ErrActorCmd
	}

	C.zsock_wait(unsafe.Pointer(a.zactorT))

	return nil
}

// Destroy destroys the auth actor.
func (a *Auth) Destroy() {
	C.zactor_destroy(&a.zactorT)
}
