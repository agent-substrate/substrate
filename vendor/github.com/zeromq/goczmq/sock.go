package goczmq

/*
#include "czmq.h"
#include <stdlib.h>
#include <string.h>

int Sock_connect(zsock_t *self, const char *format) {return zsock_connect(self, format, NULL);}
int Sock_disconnect(zsock_t *self, const char *format) {return zsock_disconnect(self, format, NULL);}
int Sock_bind(zsock_t *self, const char *format) {return zsock_bind(self, format, NULL);}
int Sock_unbind(zsock_t *self, const char *format) {return zsock_unbind(self, format, NULL);}
int Sock_sendframe(zsock_t *sock, const void *data, size_t size, int flags) {
	zframe_t *frame = zframe_new (data, size);
	int rc = zframe_send (&frame, sock, flags);
	return rc;
}
*/
import "C"

import (
	"os"
	"runtime"
	"strings"
	"unsafe"
)

// Sock wraps the CZMQ zsock class.
type Sock struct {
	zsockT    *C.struct__zsock_t
	file      string
	line      int
	zType     int
	clientIDs []string
}

func init() {
	if err := os.Setenv("ZSYS_SIGHANDLER", "false"); err != nil {
		panic(err)
	}
}

// GetLastClientID returns the id of the last client you received
// a message from if the underlying socket is a Router socket
// DEPRECATED: See goczmq.ReadWriter
func (s *Sock) GetLastClientID() []byte {
	id := []byte(s.clientIDs[0])
	s.clientIDs = s.clientIDs[1:]
	return id
}

// SetLastClientID lets you manually set the id of the client
// you last received a message from if the underlying socket
// is a Router socket
// DEPRECATED: See goczmq.ReadWriter
func (s *Sock) SetLastClientID(id []byte) {
	s.clientIDs = append(s.clientIDs, string(id))
}

// NewSock creates a new socket.  The caller source and
// line number are passed so CZMQ can report socket leaks
// intelligently.
func NewSock(t int) *Sock {
	var s *Sock
	_, file, line, ok := runtime.Caller(1)

	if ok {
		s = &Sock{
			file:      file,
			line:      line,
			zType:     t,
			clientIDs: make([]string, 0),
		}
	} else {
		s = &Sock{
			file:      "",
			line:      0,
			zType:     t,
			clientIDs: make([]string, 0),
		}
	}

	cFile := C.CString(s.file)
	defer C.free(unsafe.Pointer(cFile))

	s.zsockT = C.zsock_new_checked(C.int(s.zType), cFile, C.size_t(s.line))
	return s
}

// Connect connects a socket to an endpoint
// returns an error if the connect failed.
func (s *Sock) Connect(endpoint string) error {
	cEndpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))

	rc := C.Sock_connect(s.zsockT, cEndpoint)
	if rc != C.int(0) {
		return ErrConnect
	}
	return nil
}

// Disconnect disconnects a socket from an endpoint.  If returns
// an error if the endpoint was not found
func (s *Sock) Disconnect(endpoint string) error {
	cEndpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))

	rc := C.Sock_disconnect(s.zsockT, cEndpoint)
	if int(rc) == -1 {
		return ErrDisconnect
	}
	return nil
}

// Bind binds a socket to an endpoint.  On success returns
// the port number used for tcp transports, or 0 for other
// transports.  On failure returns a -1 for port, and an error.
func (s *Sock) Bind(endpoint string) (int, error) {
	cEndpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))

	port := C.Sock_bind(s.zsockT, cEndpoint)
	if port == C.int(-1) {
		return -1, ErrBind
	}
	return int(port), nil
}

// Unbind unbinds a socket from an endpoint.  If returns
// an error if the endpoint was not found
func (s *Sock) Unbind(endpoint string) error {
	cEndpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))

	rc := C.Sock_unbind(s.zsockT, cEndpoint)
	if int(rc) == -1 {
		return ErrUnbind
	}
	return nil
}

// Attach attaches a socket to zero or more endpoints. If endpoints is not null,
// parses as list of ZeroMQ endpoints, separated by commas, and prefixed by
// '@' (to bind the socket) or '>' (to attach the socket). If the endpoint
// does not start with '@' or '>', the serverish argument determines whether
// it is used to bind (serverish = true) or connect (serverish = false)
func (s *Sock) Attach(endpoints string, serverish bool) error {
	cEndpoints := C.CString(endpoints)
	defer C.free(unsafe.Pointer(cEndpoints))

	rc := C.zsock_attach(s.zsockT, cEndpoints, C._Bool(serverish))
	if rc == -1 {
		return ErrSockAttach
	}
	return nil
}

// NewPub creates a Pub socket and calls Attach.
// The socket will Bind by default.
func NewPub(endpoints string) (*Sock, error) {
	s := NewSock(Pub)
	return s, s.Attach(endpoints, true)
}

// NewSub creates a Sub socket and calls Attach.
// 'subscribe' is a comma delimited list of topics to subscribe to.
// The socket will Connect by default.
func NewSub(endpoints string, subscribe string) (*Sock, error) {
	s := NewSock(Sub)
	subscriptions := strings.Split(subscribe, ",")

	for _, topic := range subscriptions {
		s.SetSubscribe(topic)
	}

	return s, s.Attach(endpoints, false)
}

// NewRep creates a Rep socket and calls Attach.
// The socket will Bind by default.
func NewRep(endpoints string) (*Sock, error) {
	s := NewSock(Rep)
	return s, s.Attach(endpoints, true)
}

// NewReq creates a Req socket and calls Attach.
// The socket will Connect by default.
func NewReq(endpoints string) (*Sock, error) {
	s := NewSock(Req)
	return s, s.Attach(endpoints, false)
}

// NewPull creates a Pull socket and calls Attach.
// The socket will Bind by default.
func NewPull(endpoints string) (*Sock, error) {
	s := NewSock(Pull)
	return s, s.Attach(endpoints, true)
}

// NewPush creates a Push socket and calls Attach.
// The socket will Connect by default.
func NewPush(endpoints string) (*Sock, error) {
	s := NewSock(Push)
	return s, s.Attach(endpoints, false)
}

// NewRouter creates a Router socket and calls Attach.
// The socket will Bind by default.
func NewRouter(endpoints string) (*Sock, error) {
	s := NewSock(Router)
	return s, s.Attach(endpoints, true)
}

// NewDealer creates a Dealer socket and calls Attach.
// The socket will Connect by default.
func NewDealer(endpoints string) (*Sock, error) {
	s := NewSock(Dealer)
	return s, s.Attach(endpoints, false)
}

// NewXPub creates an XPub socket and calls Attach.
// The socket will Bind by default.
func NewXPub(endpoints string) (*Sock, error) {
	s := NewSock(XPub)
	return s, s.Attach(endpoints, true)
}

// NewXSub creates an XSub socket and calls Attach.
// The socket will Connect by default.
func NewXSub(endpoints string) (*Sock, error) {
	s := NewSock(XSub)
	return s, s.Attach(endpoints, false)
}

// NewPair creates a Pair socket and calls Attach.
// The socket will Connect by default.
func NewPair(endpoints string) (*Sock, error) {
	s := NewSock(Pair)
	return s, s.Attach(endpoints, false)
}

// NewStream creates a Stream socket and calls Attach.
// The socket will Connect by default.
func NewStream(endpoints string) (*Sock, error) {
	s := NewSock(Stream)
	return s, s.Attach(endpoints, false)
}

// Pollin returns true if there is a Pollin
// event on the socket
func (s *Sock) Pollin() bool {
	return s.Events() == Pollin
}

// Pollout returns true if there is a Pollout
// event on the socket
func (s *Sock) Pollout() bool {
	return s.Events() == Pollout
}

// SendFrame sends a byte array via the socket.  For the flags
// value, use 0 for a single message, or SNDFlagMore if it is
// a multi-part message
func (s *Sock) SendFrame(data []byte, flags int) error {
	var rc C.int
	if len(data) == 0 {
		rc = C.Sock_sendframe(s.zsockT, nil, C.size_t(0), C.int(flags))
	} else {
		rc = C.Sock_sendframe(s.zsockT, unsafe.Pointer(&data[0]), C.size_t(len(data)), C.int(flags))
	}
	if rc == C.int(-1) {
		return ErrSendFrame
	}
	return nil
}

// RecvFrame reads a frame from the socket and returns it
// as a byte array, along with a more flag and and error
// (if there is an error)
func (s *Sock) RecvFrame() ([]byte, int, error) {
	if s.zsockT == nil {
		return nil, -1, ErrRecvFrameAfterDestroy
	}

	frame := C.zframe_recv(unsafe.Pointer(s.zsockT))
	if frame == nil {
		return []byte{0}, 0, ErrRecvFrame
	}
	dataSize := C.zframe_size(frame)
	dataPtr := C.zframe_data(frame)
	b := C.GoBytes(unsafe.Pointer(dataPtr), C.int(dataSize))
	more := C.zframe_more(frame)
	C.zframe_destroy(&frame)
	return b, int(more), nil
}

// RecvFrameNoWait receives a frame from the socket
// and returns it as a byte array if one is waiting.
// Returns an empty frame, a 0 more flag and an error
// if one is not immediately available
func (s *Sock) RecvFrameNoWait() ([]byte, int, error) {
	if !s.Pollin() {
		return []byte{0}, 0, ErrRecvFrame
	}

	return s.RecvFrame()
}

// SendMessage accepts an array of byte arrays and
// sends it as a multi-part message.
func (s *Sock) SendMessage(parts [][]byte) error {
	var f int
	numParts := len(parts)
	for i, val := range parts {
		if i == numParts-1 {
			f = 0
		} else {
			f = FlagMore
		}

		err := s.SendFrame(val, f)
		if err != nil {
			return err
		}
	}
	return nil
}

// RecvMessage receives a full message from the socket
// and returns it as an array of byte arrays.
func (s *Sock) RecvMessage() ([][]byte, error) {
	var msg [][]byte

	for {
		frame, flag, err := s.RecvFrame()
		if err != nil {
			return msg, err
		}
		msg = append(msg, frame)
		if flag != FlagMore {
			break
		}
	}
	return msg, nil
}

// Read provides an io.Reader interface to a zeromq socket
// DEPRECATED: see goczmq.ReadWriter
func (s *Sock) Read(p []byte) (int, error) {
	var totalRead int
	var totalFrame int

	frame, flag, err := s.RecvFrame()
	if err != nil {
		return totalRead, err
	}

	if s.GetType() == Router {
		s.clientIDs = append(s.clientIDs, string(frame))
	} else {
		totalRead += copy(p[:], frame[:])
		totalFrame += len(frame)
	}

	for flag == FlagMore {
		frame, flag, err = s.RecvFrame()
		if err != nil {
			return totalRead, err
		}
		totalRead += copy(p[totalRead:], frame[:])
		totalFrame += len(frame)
	}

	if totalFrame > len(p) {
		err = ErrSliceFull
	} else {
		err = nil
	}

	return totalRead, err
}

// Write provides an io.Writer interface to a zeromq socket
// DEPRECATED: See goczmq.ReadWriter
func (s *Sock) Write(p []byte) (int, error) {
	var total int
	if s.GetType() == Router {
		err := s.SendFrame(s.GetLastClientID(), FlagMore)
		if err != nil {
			return total, err
		}
	}
	err := s.SendFrame(p, 0)
	if err != nil {
		return total, err
	}

	return len(p), nil
}

// RecvMessageNoWait receives a full message from the socket
// and returns it as an array of byte arrays if one is waiting.
// Returns an empty message and an error if one is not immediately
// available
func (s *Sock) RecvMessageNoWait() ([][]byte, error) {
	var msg [][]byte
	if !s.Pollin() {
		return msg, ErrRecvMessage
	}

	for {
		frame, flag, err := s.RecvFrame()
		if err != nil {
			return msg, err
		}
		msg = append(msg, frame)
		if flag != FlagMore {
			break
		}
	}
	return msg, nil
}

// GetType returns the socket's type
func (s *Sock) GetType() int {
	return s.zType
}

// Destroy destroys the underlying zsockT.
func (s *Sock) Destroy() {
	cFile := C.CString(s.file)
	defer C.free(unsafe.Pointer(cFile))

	C.zsock_destroy_checked(&s.zsockT, cFile, C.size_t(s.line))
}
