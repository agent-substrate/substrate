package goczmq

/*
#include "czmq.h"

zactor_t *Beacon_new () {
	zactor_t *beacon = zactor_new(zbeacon, NULL); return beacon;
}

int Beacon_publish(void *actor, void *data, int size, int interval) {
	return zsock_send(actor, "sbi", "PUBLISH", (byte*)data, size, interval);
}
*/
import "C"

import (
	"strconv"
	"unsafe"
)

// Beacon wraps the CZMQ beacon actor. It implements a
// peer-to-peer discovery service for local networks.  Beacons
// can broadcast and receive UDPv4 service broadcasts.
type Beacon struct {
	zactorT *C.struct__zactor_t
}

// NewBeacon creates a new Beacon instance.
func NewBeacon() *Beacon {
	z := &Beacon{}
	z.zactorT = C.Beacon_new()
	return z
}

// Verbose sets the beacon to log information to stdout.
func (b *Beacon) Verbose() error {
	cmd := C.CString("VERBOSE")
	defer C.free(unsafe.Pointer(cmd))

	rc := C.zstr_send(unsafe.Pointer(b.zactorT), cmd)
	if rc == -1 {
		return ErrActorCmd
	}

	return nil
}

// Configure accepts a port number and configures
// the beacon, returning an address
func (b *Beacon) Configure(port int) (string, error) {
	cmd := C.CString("CONFIGURE")
	defer C.free(unsafe.Pointer(cmd))

	cPort := C.CString(strconv.Itoa(port))
	defer C.free(unsafe.Pointer(cPort))

	rc := C.zstr_sendm(unsafe.Pointer(b.zactorT), cmd)
	if rc == -1 {
		return "", ErrActorCmd
	}

	rc = C.zstr_send(unsafe.Pointer(b.zactorT), cPort)
	if rc == -1 {
		return "", ErrActorCmd
	}

	cHostname := C.zstr_recv(unsafe.Pointer(b.zactorT))
	hostname := C.GoString(cHostname)

	return hostname, nil
}

// Publish publishes an announcement string at an interval
func (b *Beacon) Publish(announcement string, interval int) error {
	cmd := C.CString("PUBLISH")
	defer C.free(unsafe.Pointer(cmd))

	cAnnouncement := C.CString(announcement)
	defer C.free(unsafe.Pointer(cAnnouncement))

	cInterval := C.CString(strconv.Itoa(interval))
	defer C.free(unsafe.Pointer(cInterval))

	rc := C.zstr_sendm(unsafe.Pointer(b.zactorT), cmd)
	if rc == -1 {
		return ErrActorCmd
	}

	rc = C.zstr_sendm(unsafe.Pointer(b.zactorT), cAnnouncement)
	if rc == -1 {
		return ErrActorCmd
	}

	rc = C.zstr_send(unsafe.Pointer(b.zactorT), cInterval)
	if rc == -1 {
		return ErrActorCmd
	}

	return nil
}

// PublishBytes publishes an announcement byte slice at an interval
func (b *Beacon) PublishBytes(announcement []byte, interval int) error {
	rc := C.Beacon_publish(
		unsafe.Pointer(b.zactorT),
		unsafe.Pointer(&announcement[0]),
		C.int(len(announcement)),
		C.int(interval),
	)
	if rc == -1 {
		return ErrActorCmd
	}

	return nil
}

// Subscribe subscribes to beacons matching the filter
func (b *Beacon) Subscribe(filter string) error {
	cmd := C.CString("SUBSCRIBE")
	defer C.free(unsafe.Pointer(cmd))

	cFilter := C.CString(filter)
	defer C.free(unsafe.Pointer(cFilter))

	rc := C.zstr_sendm(unsafe.Pointer(b.zactorT), cmd)
	if rc == -1 {
		return ErrActorCmd
	}

	rc = C.zstr_send(unsafe.Pointer(b.zactorT), cFilter)
	if rc == -1 {
		return ErrActorCmd
	}

	return nil
}

// Recv waits for the specific timeout in milliseconds to receive a beacon
func (b *Beacon) Recv(timeout int) [][]byte {
	C.zsock_set_rcvtimeo(unsafe.Pointer(b.zactorT), C.int(timeout))

	cAddrFrame := C.zframe_recv(unsafe.Pointer(b.zactorT))
	defer C.zframe_destroy(&cAddrFrame)
	if cAddrFrame == nil {
		return nil
	}
	addr := C.GoBytes(unsafe.Pointer(C.zframe_data(cAddrFrame)), C.int(C.zframe_size(cAddrFrame)))

	cBeaconFrame := C.zframe_recv(unsafe.Pointer(b.zactorT))
	defer C.zframe_destroy(&cBeaconFrame)
	if cBeaconFrame == nil {
		return nil
	}
	beacon := C.GoBytes(unsafe.Pointer(C.zframe_data(cBeaconFrame)), C.int(C.zframe_size(cBeaconFrame)))

	return [][]byte{addr, beacon}
}

// Destroy destroys the beacon.
func (b *Beacon) Destroy() {
	C.zactor_destroy(&b.zactorT)
}
