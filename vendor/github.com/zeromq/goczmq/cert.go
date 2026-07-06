package goczmq

/*
#include "czmq.h"

void Set_meta(zcert_t *self, const char *key, const char *value) {zcert_set_meta(self, key, "%s", value);}
*/
import "C"

import (
	"fmt"
	"os"
	"unsafe"
)

// Cert wraps the CZMQ zcert class. It provides tools for
// creating and working with ZMQ CURVE security certs.
// The certs can be used as a temporary object in memory
// or persisted to disk. Certs are made up of a public
// and secret keypair + metadata.
type Cert struct {
	zcertT *C.struct__zcert_t
}

// NewCert creates a new empty Cert instance
func NewCert() *Cert {
	return &Cert{
		zcertT: C.zcert_new(),
	}
}

// NewCertFromKeys creates a new Cert from a public and private key
func NewCertFromKeys(public []byte, secret []byte) (*Cert, error) {
	if len(public) != 32 {
		return nil, fmt.Errorf("invalid public key")
	}

	if len(secret) != 32 {
		return nil, fmt.Errorf("invalid private key")
	}

	return &Cert{
		zcertT: C.zcert_new_from(
			(*C.byte)(unsafe.Pointer(&public[0])),
			(*C.byte)(unsafe.Pointer(&secret[0]))),
	}, nil
}

// NewCertFromFile Load loads a Cert from files
func NewCertFromFile(filename string) (*Cert, error) {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return nil, ErrCertNotFound
	}

	cFilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cFilename))

	cert := C.zcert_load(cFilename)
	return &Cert{
		zcertT: cert,
	}, nil
}

// SetMeta sets meta data for a Cert
func (c *Cert) SetMeta(key string, value string) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))

	C.Set_meta(c.zcertT, cKey, cValue)
}

// Meta returns a meta data item from a Cert given a key
func (c *Cert) Meta(key string) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	val := C.zcert_meta(c.zcertT, cKey)
	return C.GoString(val)
}

// PublicText returns the public key as a string
func (c *Cert) PublicText() string {
	val := C.zcert_public_txt(c.zcertT)
	return C.GoString(val)
}

// Apply sets the public and private keys for a socket
func (c *Cert) Apply(s *Sock) {
	handle := C.zsock_resolve(unsafe.Pointer(s.zsockT))
	C.zsock_set_curve_secretkey_bin(handle, C.zcert_secret_key(c.zcertT))
	C.zsock_set_curve_publickey_bin(handle, C.zcert_public_key(c.zcertT))
}

// Dup duplicates a Cert
func (c *Cert) Dup() *Cert {
	return &Cert{
		zcertT: C.zcert_dup(c.zcertT),
	}
}

// Equal checks two Certs for equality
func (c *Cert) Equal(compare *Cert) bool {
	check := C.zcert_eq(c.zcertT, compare.zcertT)
	if check == C.bool(true) {
		return true
	}
	return false
}

// Print prints a Cert to stdout
func (c *Cert) Print() {
	C.zcert_print(c.zcertT)
}

// SavePublic saves the public key to a file
func (c *Cert) SavePublic(filename string) error {
	cFilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cFilename))

	rc := C.zcert_save_public(c.zcertT, cFilename)
	if rc == C.int(-1) {
		return fmt.Errorf("SavePublic error")
	}
	return nil
}

// SaveSecret saves the secret key to a file
func (c *Cert) SaveSecret(filename string) error {
	cFilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cFilename))

	rc := C.zcert_save_secret(c.zcertT, cFilename)
	if rc == C.int(-1) {
		return fmt.Errorf("SaveSecret error")
	}
	return nil
}

// Save saves the public and secret key to filename and filename_secret
func (c *Cert) Save(filename string) error {
	cFilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cFilename))

	rc := C.zcert_save(c.zcertT, cFilename)
	if rc == C.int(-1) {
		return fmt.Errorf("SavePublic: error")
	}
	return nil
}

// Destroy destroys Cert instance
func (c *Cert) Destroy() {
	C.zcert_destroy(&c.zcertT)
}
