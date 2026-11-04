package zmtp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Connection is a ZMTP level connection
type Connection struct {
	rw                         io.ReadWriter
	metadata                   map[string]string
	securityMechanism          SecurityMechanism
	socket                     Socket
	isPrepared                 bool
	isDowngraded               bool
	asServer, otherEndAsServer bool
}

// SocketType is a ZMTP socket type
type SocketType string

// SocketIdentity is the ZMTP metadata socket identity.
// See:
//  https://rfc.zeromq.org/spec:23/ZMTP/.
type SocketIdentity []byte

func (id SocketIdentity) String() string {
	n := len(id)
	if n > 255 { // ZMTP identities are: 0*255OCTET
		n = 255
	}
	return string(id[:n])
}

const (
	ClientSocketType SocketType = "CLIENT" // a ZMQ_CLIENT socket
	ServerSocketType SocketType = "SERVER" // a ZMQ_SERVER socket
	PullSocketType   SocketType = "PULL"   // a ZMQ_PULL socket
	PushSocketType   SocketType = "PUSH"   // a ZMQ_PUSH socket
	DealerSocketType SocketType = "DEALER" // a ZMQ_DEALER socket
	RouterSocketType SocketType = "ROUTER" // a ZMQ_ROUTER socket
	ReqSocketType    SocketType = "REQ"    // a ZMQ_REQ socket
	RepSocketType    SocketType = "REP"    // a ZMQ_REP socket
	PubSocketType    SocketType = "PUB"    // a ZMQ_PUB socket
	SubSocketType    SocketType = "SUB"    // a ZMQ_SUB socket
	XPubSocketType   SocketType = "XPUB"   // a ZMQ_XPUB socket
	XSubSocketType   SocketType = "XSUB"   // a ZMQ_XSUB socket
)

// NewConnection accepts an io.ReadWriter and creates a new ZMTP connection
func NewConnection(rw io.ReadWriter) *Connection {
	return &Connection{
		rw:       rw,
		metadata: make(map[string]string),
	}
}

// Prepare performs a ZMTP handshake over a Connection's readWriter
func (c *Connection) Prepare(mechanism SecurityMechanism, socketType SocketType, socketID SocketIdentity, asServer bool, applicationMetadata map[string]string) (map[string]string, error) {
	if c.isPrepared {
		return nil, errors.New("Connection was already prepared")
	}

	c.isPrepared = true
	c.securityMechanism = mechanism

	var err error
	if c.socket, err = NewSocket(socketType); err != nil {
		return nil, fmt.Errorf("gomq/zmtp: Got error while creating socket: %v", err)
	}

	// Send/recv greeting
	if err := c.sendGreeting(asServer); err != nil {
		return nil, fmt.Errorf("gomq/zmtp: Got error while sending greeting: %v", err)
	}
	if err := c.recvGreeting(asServer); err != nil {
		return nil, fmt.Errorf("gomq/zmtp: Got error while receiving greeting: %v", err)
	}

	// Do security handshake
	if err := mechanism.Handshake(); err != nil {
		return nil, fmt.Errorf("gomq/zmtp: Got error while running the security handshake: %v", err)
	}

	// Send/recv metadata
	if err := c.sendMetadata(socketType, socketID, applicationMetadata); err != nil {
		return nil, fmt.Errorf("gomq/zmtp: Got error while sending metadata: %v", err)
	}

	otherEndApplicationMetaData, err := c.recvMetadata()
	if err != nil {
		return nil, fmt.Errorf("gomq/zmtp: Got error while receiving metadata: %v", err)
	}

	return otherEndApplicationMetaData, nil
}

func (c *Connection) sendGreeting(asServer bool) error {
	greeting := greeting{
		SignaturePrefix: signaturePrefix,
		SignatureSuffix: signatureSuffix,
		Version:         version,
	}
	toNullPaddedString(string(c.securityMechanism.Type()), greeting.Mechanism[:])

	if err := greeting.marshal(c.rw); err != nil {
		return err
	}

	return nil
}

func (c *Connection) recvGreeting(asServer bool) error {
	var greeting greeting

	if err := greeting.unmarshal(c.rw); err != nil {
		return fmt.Errorf("Error while reading: %v", err)
	}

	if greeting.SignaturePrefix != signaturePrefix {
		return fmt.Errorf("Signature prefix received does not correspond with expected signature. Received: %#v. Expected: %#v.", greeting.SignaturePrefix, signaturePrefix)
	}

	if greeting.SignatureSuffix != signatureSuffix {
		return fmt.Errorf("Signature prefix received does not correspond with expected signature. Received: %#v. Expected: %#v.", greeting.SignatureSuffix, signatureSuffix)
	}

	if greeting.Version != version {
		if greeting.Version[0] == version[0] && greeting.Version[1] < version[1] {
			c.isDowngraded = true
		} else {
			return fmt.Errorf("Version %v.%v received does match expected version %v.%v", int(greeting.Version[0]), int(greeting.Version[1]), int(majorVersion), int(minorVersion))
		}
	}

	var otherMechanism = fromNullPaddedString(greeting.Mechanism[:])
	var thisMechanism = string(c.securityMechanism.Type())
	if thisMechanism != otherMechanism {
		return fmt.Errorf("Encryption mechanism on other side %q does not match this side's %q", otherMechanism, thisMechanism)
	}

	otherEndAsServer, err := fromByteBool(greeting.ServerFlag)
	if err != nil {
		return err
	}
	c.otherEndAsServer = otherEndAsServer

	return nil
}

func (c *Connection) sendMetadata(socketType SocketType, socketID SocketIdentity, applicationMetadata map[string]string) error {
	buffer := new(bytes.Buffer)
	var usedKeys map[string]struct{}

	for k, v := range applicationMetadata {
		if len(k) == 0 {
			return errors.New("Cannot send empty application metadata key")
		}

		lowerCaseKey := strings.ToLower(k)
		if _, alreadyPresent := usedKeys[lowerCaseKey]; alreadyPresent {
			return fmt.Errorf("Key %q is specified multiple times with different casing", lowerCaseKey)
		}

		usedKeys[lowerCaseKey] = struct{}{}
		c.writeMetadata(buffer, "x-"+lowerCaseKey, v)
	}

	c.writeMetadata(buffer, "socket-type", string(socketType))
	c.writeMetadata(buffer, "Identity", socketID.String())

	return c.SendCommand("READY", buffer.Bytes())
}

func (c *Connection) writeMetadata(buffer *bytes.Buffer, name string, value string) {
	var (
		p        = 0
		nameLen  = len(name)
		valueLen = len(value)
		buf      = make([]byte, 1+nameLen+4+valueLen)
	)
	buf[p] = byte(nameLen)
	p++
	p += copy(buf[p:], name)
	byteOrder.PutUint32(buf[p:p+4], uint32(valueLen))
	p += 4
	copy(buf[p:], value)

	_, err := buffer.Write(buf)
	if err != nil {
		panic(err)
	}
}

func (c *Connection) recvMetadata() (map[string]string, error) {
	isCommand, body, err := c.read()
	if err != nil {
		return nil, err
	}

	if !isCommand {
		return nil, errors.New("Got a message frame for metadata, expected a command frame")
	}

	command, err := c.parseCommand(body)
	if err != nil {
		return nil, err
	}

	if command.Name != "READY" {
		return nil, fmt.Errorf("Got a %v command for metadata instead of the expected READY command frame", command.Name)
	}

	applicationMetadata := make(map[string]string)
	i := 0
	for i < len(command.Body) {
		// Key length
		keyLength := int(command.Body[i])
		if i+keyLength >= len(command.Body) {
			return nil, fmt.Errorf("metadata key of length %v overflows body of length %v at position %v", keyLength, len(command.Body), i)
		}
		i++

		// Key
		key := strings.ToLower(string(command.Body[i : i+keyLength]))
		i += keyLength

		// Value length
		rawValueLength := byteOrder.Uint32(command.Body[i : i+4])

		if uint64(rawValueLength) > uint64(maxInt) {
			return nil, fmt.Errorf("Length of value %v overflows integer max length %v on this platform", rawValueLength, maxInt)
		}

		valueLength := int(rawValueLength)
		if i+valueLength >= len(command.Body) {
			return nil, fmt.Errorf("metadata value of length %v overflows body of length %v at position %v", valueLength, len(command.Body), i)
		}
		i += 4

		// Value
		value := string(command.Body[i : i+valueLength])
		i += valueLength

		if strings.HasPrefix(key, "x-") {
			applicationMetadata[key[2:]] = value
		} else {
			c.metadata[key] = value
		}
	}

	socketType := c.metadata["socket-type"]
	if !c.socket.IsSocketTypeCompatible(SocketType(socketType)) {
		return nil, fmt.Errorf("Socket type %v is not compatible with %v", c.socket.Type(), socketType)
	}

	return applicationMetadata, nil
}

// GetIdentity get connection's identity
func (c *Connection) GetIdentity() (string, error) {
	if identity, ok := c.metadata["identity"]; ok {
		return identity, nil
	}
	return "", fmt.Errorf("Indentiy not exist")
}

// SendCommand sends a ZMTP command over a Connection
func (c *Connection) SendCommand(commandName string, body []byte) error {
	cmdLen := len(commandName)
	if cmdLen > 255 {
		return errors.New("Command names may not be longer than 255 characters")
	}

	bodyLen := len(body)

	buf := make([]byte, 1+cmdLen+bodyLen) // FIXME(sbinet): maybe use a pool of []byte ?
	buf[0] = byte(cmdLen)
	copy(buf[1:], []byte(commandName))
	copy(buf[1+cmdLen:], body)

	return c.send(true, buf)
}

// SendFrame sends a ZMTP frame over a Connection
func (c *Connection) SendFrame(body []byte) error {
	return c.send(false, body)
}

func (c *Connection) send(isCommand bool, body []byte) error {
	// Compute total body length
	length := len(body)

	var bitFlags byte

	// More flag: Unused, we don't support multiframe messages

	// Long flag
	isLong := length > 255
	if isLong {
		bitFlags ^= isLongBitFlag
	}

	// Command flag
	if isCommand {
		bitFlags ^= isCommandBitFlag
	}

	// Write out the message itself
	if _, err := c.rw.Write([]byte{bitFlags}); err != nil {
		return err
	}

	if isLong {
		var buf [8]byte
		byteOrder.PutUint64(buf[:], uint64(len(body)))
		if _, err := c.rw.Write(buf[:]); err != nil {
			return err
		}
	} else {
		if _, err := c.rw.Write([]byte{uint8(len(body))}); err != nil {
			return err
		}
	}

	if _, err := c.rw.Write(c.securityMechanism.Encrypt(body)); err != nil {
		return err
	}

	return nil
}

// Recv starts listening to the ReadWriter and passes *Message to a channel
func (c *Connection) Recv(messageOut chan<- *Message) {
	go func() {
		for {
			// Actually read out the body and send it over the channel now
			isCommand, body, err := c.read()
			if err != nil {
				messageOut <- &Message{Err: err, MessageType: ErrorMessage}
				return
			}

			if !isCommand {
				// Data frame
				frames := [][]byte{body}
				messageOut <- &Message{Body: frames, MessageType: UserMessage}
			} else {
				command, err := c.parseCommand(body)
				if err != nil {
					messageOut <- &Message{Err: err, MessageType: ErrorMessage}
					return
				}

				// Check what type of command we got
				// Certain commands we deal with directly, the rest we send over to the application
				switch command.Name {
				case "PING":
					// When we get a ping, we want to send back a pong, we don't really care about the contents right now
					if err := c.SendCommand("PONG", nil); err != nil {
						messageOut <- &Message{Err: err, MessageType: ErrorMessage}
						return
					}
				default:
					frames := [][]byte{command.Body}
					messageOut <- &Message{Name: command.Name, Body: frames, MessageType: ErrorMessage}
				}

			}
		}
	}()
}

// read returns the isCommand flag, the body of the message, and optionally an error
func (c *Connection) read() (bool, []byte, error) {
	var header [2]byte
	var longLength [8]byte

	// Read out the header
	_, err := io.ReadFull(c.rw, header[:])
	if err != nil {
		return false, nil, err
	}

	bitFlags := header[0]

	// Read all the flags
	hasMore := bitFlags&hasMoreBitFlag == hasMoreBitFlag
	isLong := bitFlags&isLongBitFlag == isLongBitFlag
	isCommand := bitFlags&isCommandBitFlag == isCommandBitFlag

	// Error out in case get a more flag set to true
	if hasMore {
		return false, nil, errors.New("Received a packet with the MORE flag set to true, we don't support more")
	}

	// Determine the actual length of the body
	bodyLength := uint64(0)
	if isLong {
		// We read 2 bytes of the header already
		// In case of a long message, the length is bytes 2-8 of the header
		// We already have the first byte, so assign it, and then read the rest
		longLength[0] = header[1]

		_, err := io.ReadFull(c.rw, longLength[1:])
		if err != nil {
			return false, nil, err
		}

		bodyLength = byteOrder.Uint64(longLength[:])
	} else {
		// Short message length is just 1 byte, read it
		bodyLength = uint64(header[1])
	}

	if bodyLength > uint64(maxInt64) {
		return false, nil, fmt.Errorf("Body length %v overflows max int64 value %v", bodyLength, maxInt64)
	}

	buf := make([]byte, bodyLength)
	_, err = io.ReadFull(c.rw, buf)
	if err != nil {
		return false, nil, err
	}
	return isCommand, buf, nil
}

func (c *Connection) parseCommand(body []byte) (*Command, error) {
	// Sanity check
	if len(body) == 0 {
		return nil, errors.New("Got empty command frame body")
	}

	// Read out the command length
	commandNameLength := int(body[0])
	if commandNameLength > len(body)-1 {
		return nil, fmt.Errorf("Got command name length %v, which is too long for a body of length %v", commandNameLength, len(body))
	}

	command := &Command{
		Name: string(body[1 : commandNameLength+1]),
		Body: body[1+commandNameLength:],
	}

	return command, nil
}

func (c *Connection) SendMultipart(bs [][]byte) error {
	const cmd = false
	return c.sendMultipart(cmd, bs)
}

func (c *Connection) sendMultipart(isCommand bool, bs [][]byte) error {
	for i, part := range bs {
		// Compute total body length
		length := len(part)

		var bitFlags byte

		// More flag:
		if i < len(bs)-1 {
			bitFlags ^= hasMoreBitFlag
		}

		// Long flag
		isLong := length > 255
		if isLong {
			bitFlags ^= isLongBitFlag
		}

		// Command flag
		if isCommand {
			bitFlags ^= isCommandBitFlag
		}

		// Write out the message itself
		if _, err := c.rw.Write([]byte{bitFlags}); err != nil {
			return err
		}

		if isLong {
			var buf [8]byte
			byteOrder.PutUint64(buf[:], uint64(len(part)))
			if _, err := c.rw.Write(buf[:]); err != nil {
				return err
			}
		} else {
			if _, err := c.rw.Write([]byte{uint8(len(part))}); err != nil {
				return err
			}
		}

		if _, err := c.rw.Write(c.securityMechanism.Encrypt(part)); err != nil {
			return err
		}
	}
	return nil
}

// RecvMultipart starts listening to the ReadWriter and passes *Message to a channel
func (c *Connection) RecvMultipart(messageOut chan<- *Message) {
	go func() {
		for {
			// Actually read out the body and send it over the channel now
			isCommand, body, err := c.readMultipart()
			if err != nil {
				messageOut <- &Message{Err: err, MessageType: ErrorMessage}
				return
			}

			if !isCommand {
				// Data frame
				messageOut <- &Message{Body: body, MessageType: UserMessage}
			} else {
				command, err := c.parseCommand(body[0])
				if err != nil {
					messageOut <- &Message{Err: err, MessageType: ErrorMessage}
					return
				}

				// Check what type of command we got
				// Certain commands we deal with directly, the rest we send over to the application
				switch command.Name {
				case "PING":
					// When we get a ping, we want to send back a pong, we don't really care about the contents right now
					if err := c.SendCommand("PONG", nil); err != nil {
						messageOut <- &Message{Err: err, MessageType: ErrorMessage}
						return
					}
				default:
					frames := [][]byte{command.Body}
					messageOut <- &Message{Name: command.Name, Body: frames, MessageType: ErrorMessage}
				}

			}
		}
	}()
}

// readMultipart returns the isCommand flag, the body of the message, and optionally an error
func (c *Connection) readMultipart() (bool, [][]byte, error) {
	var (
		header     [2]byte
		longLength [8]byte
		frames     [][]byte

		hasMore   = true
		isCommand = false
	)

	for hasMore {
		// Read out the header
		_, err := io.ReadFull(c.rw, header[:])
		if err != nil {
			return false, nil, err
		}

		bitFlags := header[0]

		// Read all the flags
		hasMore = bitFlags&hasMoreBitFlag == hasMoreBitFlag
		isLong := bitFlags&isLongBitFlag == isLongBitFlag
		isCommand = isCommand || (bitFlags&isCommandBitFlag == isCommandBitFlag)

		// Determine the actual length of the body
		bodyLength := uint64(0)
		if isLong {
			// We read 2 bytes of the header already
			// In case of a long message, the length is bytes 2-8 of the header
			// We already have the first byte, so assign it, and then read the rest
			longLength[0] = header[1]

			_, err := io.ReadFull(c.rw, longLength[1:])
			if err != nil {
				return false, nil, err
			}

			bodyLength = byteOrder.Uint64(longLength[:])
		} else {
			// Short message length is just 1 byte, read it
			bodyLength = uint64(header[1])
		}

		if bodyLength > uint64(maxInt64) {
			return false, nil, fmt.Errorf("Body length %v overflows max int64 value %v", bodyLength, maxInt64)
		}

		buf := make([]byte, bodyLength)
		_, err = io.ReadFull(c.rw, buf)
		if err != nil {
			return false, nil, err
		}
		frames = append(frames, buf)
	}

	return isCommand, frames, nil
}
