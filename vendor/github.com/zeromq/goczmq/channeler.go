package goczmq

/*
#include "czmq.h"
void Sock_init() {zsys_init();}
*/
import "C"

import (
	"fmt"
	"math/rand"
	"strings"
)

// Channeler serializes all access to a socket through a send
// and receive channel.  It starts two threads, on is used for receiving
// from the zeromq socket.  The other is used to listen to the receive
// channel, and send everything back to the socket thrad for sending
// using an additional inproc socket.
type Channeler struct {
	id          int64
	sockType    int
	endpoints   string
	subscribe   *string
	commandAddr string
	proxyAddr   string
	commandChan chan<- string
	SendChan    chan<- [][]byte
	RecvChan    <-chan [][]byte
}

// Destroy sends a message to the Channeler to shut it down
// and clean it up.
func (c *Channeler) Destroy() {
	c.commandChan <- "destroy"
}

// Subscribe to a Topic
func (c *Channeler) Subscribe(topic string) {
	c.commandChan <- fmt.Sprintf("subscribe %s", topic)
}

// Unsubscribe from a Topic
func (c *Channeler) Unsubscribe(topic string) {
	c.commandChan <- fmt.Sprintf("unsubscribe %s", topic)
}

// actor is a routine that handles communication with
// the zeromq socket.
func (c *Channeler) actor(recvChan chan<- [][]byte) {
	pipe, err := NewPair(fmt.Sprintf(">%s", c.commandAddr))
	if err != nil {
		panic(err)
	}
	defer pipe.Destroy()
	defer close(recvChan)

	pull, err := NewPull(c.proxyAddr)
	if err != nil {
		panic(err)
	}
	defer pull.Destroy()

	sock := NewSock(c.sockType)
	defer sock.Destroy()
	switch c.sockType {
	case Pub, Rep, Pull, Router, XPub:
		err = sock.Attach(c.endpoints, true)
		if err != nil {
			panic(err)
		}

	case Req, Push, Dealer, Pair, Stream, XSub:
		err = sock.Attach(c.endpoints, false)
		if err != nil {
			panic(err)
		}

	case Sub:
		if c.subscribe != nil {
			subscriptions := strings.Split(*c.subscribe, ",")
			for _, topic := range subscriptions {
				sock.SetSubscribe(topic)
			}
		}

		err = sock.Attach(c.endpoints, false)
		if err != nil {
			panic(err)
		}

	default:
		panic(ErrInvalidSockType)
	}

	poller, err := NewPoller(sock, pull, pipe)
	if err != nil {
		panic(err)
	}
	defer poller.Destroy()

	for {
		s := poller.Wait(-1)
		switch s {
		case pipe:
			cmd, err := pipe.RecvMessage()
			if err != nil {
				panic(err)
			}

			switch string(cmd[0]) {
			case "destroy":
				disconnect := strings.Split(c.endpoints, ",")
				for _, endpoint := range disconnect {
					sock.Disconnect(endpoint)
				}
				pipe.SendMessage([][]byte{[]byte("ok")})
				goto ExitActor
			case "subscribe":
				topic := string(cmd[1])
				sock.SetSubscribe(topic)
				pipe.SendMessage([][]byte{[]byte("ok")})
			case "unsubscribe":
				topic := string(cmd[1])
				sock.SetUnsubscribe(topic)
				pipe.SendMessage([][]byte{[]byte("ok")})
			}

		case sock:
			msg, err := s.RecvMessage()
			if err != nil {
				panic(err)
			}
			recvChan <- msg

		case pull:
			msg, err := pull.RecvMessage()
			if err != nil {
				panic(err)
			}

			err = sock.SendMessage(msg)
			if err != nil {
				panic(err)
			}
		}
	}
ExitActor:
}

// channeler is a routine that handles the channel select loop
// and sends commands to the zeromq socket.
func (c *Channeler) channeler(commandChan <-chan string, sendChan <-chan [][]byte) {
	push, err := NewPush(c.proxyAddr)
	if err != nil {
		panic(err)
	}
	defer push.Destroy()

	pipe, err := NewPair(fmt.Sprintf("@%s", c.commandAddr))
	if err != nil {
		panic(err)
	}
	defer pipe.Destroy()

	for {
		select {
		case cmd := <-commandChan:
			switch cmd {
			case "destroy":
				err = pipe.SendFrame([]byte("destroy"), FlagNone)
				if err != nil {
					panic(err)
				}
				_, err = pipe.RecvMessage()
				if err != nil {
					panic(err)
				}
				goto ExitChanneler
			default:
				parts := strings.Split(cmd, " ")
				numParts := len(parts)
				message := make([][]byte, numParts, numParts)
				for i, p := range parts {
					message[i] = []byte(p)
				}
				err := pipe.SendMessage(message)
				if err != nil {
					panic(err)
				}
				_, err = pipe.RecvMessage()
				if err != nil {
					panic(err)
				}
			}

		case msg := <-sendChan:
			err := push.SendMessage(msg)
			if err != nil {
				panic(err)
			}
		}
	}
ExitChanneler:
}

// newChanneler accepts arguments from the socket type based
// constructors and creates a new Channeler instance
func newChanneler(sockType int, endpoints string, subscribe ...string) *Channeler {
	commandChan := make(chan string)
	sendChan := make(chan [][]byte)
	recvChan := make(chan [][]byte)

	C.Sock_init()
	c := &Channeler{
		id:          rand.Int63(),
		endpoints:   endpoints,
		sockType:    sockType,
		commandChan: commandChan,
		SendChan:    sendChan,
		RecvChan:    recvChan,
	}
	c.commandAddr = fmt.Sprintf("inproc://actorcontrol%d", c.id)
	c.proxyAddr = fmt.Sprintf("inproc://proxy%d", c.id)

	if len(subscribe) > 0 {
		topics := strings.Join(subscribe, ",")
		c.subscribe = &topics
	}

	go c.channeler(commandChan, sendChan)
	go c.actor(recvChan)

	return c
}

// NewPubChanneler creats a new Channeler wrapping
// a Pub socket.  The socket will bind by default.
func NewPubChanneler(endpoints string) *Channeler {
	return newChanneler(Pub, endpoints, "")
}

// NewSubChanneler creates a new Channeler wrapping
// a Sub socket. Along with an endpoint list
// it accepts a comma delimited list of topics.
// The socket will connect by default.
func NewSubChanneler(endpoints string, subscribe ...string) *Channeler {
	return newChanneler(Sub, endpoints, subscribe...)
}

// NewRepChanneler creates a new Channeler wrapping
// a Rep socket. The socket will bind by default.
func NewRepChanneler(endpoints string) *Channeler {
	return newChanneler(Rep, endpoints, "")
}

// NewReqChanneler creates a new Channeler wrapping
// a Req socket. The socket will connect by default.
func NewReqChanneler(endpoints string) *Channeler {
	return newChanneler(Req, endpoints, "")
}

// NewPullChanneler creates a new Channeler wrapping
// a Pull socket. The socket will bind by default.
func NewPullChanneler(endpoints string) *Channeler {
	return newChanneler(Pull, endpoints, "")
}

// NewPushChanneler creates a new Channeler wrapping
// a Push socket. The socket will connect by default.
func NewPushChanneler(endpoints string) *Channeler {
	return newChanneler(Push, endpoints, "")
}

// NewRouterChanneler creates a new Channeler wrapping
// a Router socket. The socket will Bind by default.
func NewRouterChanneler(endpoints string) *Channeler {
	return newChanneler(Router, endpoints, "")
}

// NewDealerChanneler creates a new Channeler wrapping
// a Dealer socket. The socket will connect by default.
func NewDealerChanneler(endpoints string) *Channeler {
	return newChanneler(Dealer, endpoints, "")
}

// NewXPubChanneler creates a new Channeler wrapping
// an XPub socket. The socket will Bind by default.
func NewXPubChanneler(endpoints string) *Channeler {
	return newChanneler(XPub, endpoints, "")
}

// NewXSubChanneler creates a new Channeler wrapping
// a XSub socket. The socket will connect by default.
func NewXSubChanneler(endpoints string) *Channeler {
	return newChanneler(XSub, endpoints, "")
}

// NewPairChanneler creates a new Channeler wrapping
// a Pair socket. The socket will connect by default.
func NewPairChanneler(endpoints string) *Channeler {
	return newChanneler(Pair, endpoints, "")
}

// NewStreamChanneler creates a new Channeler wrapping
// a Pair socket. The socket will connect by default.
func NewStreamChanneler(endpoints string) *Channeler {
	return newChanneler(Stream, endpoints, "")
}
