package zmtp

import (
	"encoding/binary"
	"io"
)

const (
	majorVersion uint8 = 3
	minorVersion uint8 = 1
)

const (
	signaturePrefix = 0xFF
	signatureSuffix = 0x7F
)

const (
	hasMoreBitFlag   = 0x1
	isLongBitFlag    = 0x2
	isCommandBitFlag = 0x4
)

// MessageType represents a "type" of ZMTP message
// (User, Command, Error)
type MessageType int

const (
	// UserMessage is a ZMTP message sent by a user
	UserMessage MessageType = iota

	// CommandMessage is a ZMTP command
	CommandMessage

	// ErrorMessage is.. an error message
	ErrorMessage
)

var (
	version = [2]uint8{majorVersion, minorVersion}
)

var byteOrder = binary.BigEndian

const maxUint = ^uint(0)
const minUint = 0
const maxInt = int(maxUint >> 1)
const minInt = -maxInt - 1
const maxUint64 = ^uint64(0)
const minUint64 = 0
const maxInt64 = int64(maxUint64 >> 1)
const minInt64 = -maxInt64 - 1

type greeting struct {
	SignaturePrefix byte
	_               [8]byte
	SignatureSuffix byte
	Version         [2]uint8
	Mechanism       [20]byte
	ServerFlag      byte
	_               [31]byte
}

func (g *greeting) unmarshal(r io.Reader) error {
	var buf [64]byte
	_, err := io.ReadFull(r, buf[:])
	if err != nil {
		return err
	}
	g.SignaturePrefix = buf[0]
	// padding 1 ignored
	g.SignatureSuffix = buf[9]
	g.Version[0] = buf[10]
	g.Version[1] = buf[11]
	copy(g.Mechanism[:], buf[12:32])
	g.ServerFlag = buf[32]
	// padding 2 ignored

	return nil
}

func (g *greeting) marshal(w io.Writer) error {
	var buf [64]byte
	buf[0] = g.SignaturePrefix
	// padding 1 ignored
	buf[9] = g.SignatureSuffix
	buf[10] = g.Version[0]
	buf[11] = g.Version[1]
	copy(buf[12:32], g.Mechanism[:])
	buf[32] = g.ServerFlag
	// padding 2 ignored

	_, err := w.Write(buf[:])
	return err
}

// Command represents an underlying ZMTP command
type Command struct {
	Index int
	Name  string
	Body  []byte
}

// Message represents a ZMTP message
type Message struct {
	Index       int
	Name        string
	Body        [][]byte
	Err         error
	MessageType MessageType
}
