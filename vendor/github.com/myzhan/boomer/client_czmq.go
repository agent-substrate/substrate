// +build goczmq

package boomer

import (
	"fmt"
	"log"

	"github.com/zeromq/goczmq"
)

type czmqSocketClient struct {
	masterHost string
	masterPort int
	identity   string

	dealerSocket *goczmq.Sock

	fromMaster             chan message
	toMaster               chan message
	disconnectedFromMaster chan bool
	shutdownChan           chan bool
}

func newClient(masterHost string, masterPort int, identity string) (client *czmqSocketClient) {
	log.Println("Boomer is built with goczmq support.")
	client = &czmqSocketClient{
		masterHost:             masterHost,
		masterPort:             masterPort,
		identity:               identity,
		fromMaster:             make(chan message, 100),
		toMaster:               make(chan message, 100),
		disconnectedFromMaster: make(chan bool),
		shutdownChan:           make(chan bool),
	}

	return client
}

func (c *czmqSocketClient) connect() (err error) {
	addr := fmt.Sprintf("tcp://%s:%d", c.masterHost, c.masterPort)
	dealer := goczmq.NewSock(goczmq.Dealer)
	dealer.SetOption(goczmq.SockSetIdentity(c.identity))
	err = dealer.Connect(addr)
	if err != nil {
		return err
	}

	c.dealerSocket = dealer

	log.Printf("Boomer is connected to master(%s) press Ctrl+c to quit.\n", addr)

	go c.recv()
	go c.send()

	return nil
}

func (c *czmqSocketClient) close() {
	close(c.shutdownChan)
	if c.dealerSocket != nil {
		c.dealerSocket.Destroy()
	}
}

func (c *czmqSocketClient) recvChannel() chan message {
	return c.fromMaster
}

func (c *czmqSocketClient) recv() {
	for {
		select {
		case <-c.shutdownChan:
			return
		default:
			msg, _, err := c.dealerSocket.RecvFrame()
			if err != nil {
				log.Printf("Error reading: %v\n", err)
				continue
			}

			decodedGenericMsg, err := newGenericMessageFromBytes(msg)
			if err == nil {
				if decodedGenericMsg.NodeID != c.identity {
					log.Printf("Recv a %s message for node(%s), not for me(%s), dropped.\n", decodedGenericMsg.Type, decodedGenericMsg.NodeID, c.identity)
				} else {
					c.fromMaster <- decodedGenericMsg
				}
				continue
			}

			decodedCustomMsg, err := newCustomMessageFromBytes(msg)
			if err == nil {
				if decodedCustomMsg.NodeID != c.identity {
					log.Printf("Recv a %s message for node(%s), not for me(%s), dropped.\n", decodedCustomMsg.Type, decodedCustomMsg.NodeID, c.identity)
				} else {
					c.fromMaster <- decodedCustomMsg
				}
				continue
			}

			log.Printf("Msgpack decode fail: %v\n", err)
		}
	}
}

func (c *czmqSocketClient) sendChannel() chan message {
	return c.toMaster
}

func (c *czmqSocketClient) send() {
	for {
		select {
		case <-c.shutdownChan:
			return
		case msg := <-c.toMaster:
			c.sendMessage(msg)

			// If we send a genericMessage and the message type is quit, we need to disconnect.
			m, ok := msg.(*genericMessage)
			if ok {
				if m.Type == "quit" {
					c.disconnectedFromMaster <- true
				}
			}
		}
	}
}

func (c *czmqSocketClient) sendMessage(msg message) {
	serializedMessage, err := msg.serialize()
	if err != nil {
		log.Printf("Msgpack encode fail: %v\n", err)
		return
	}
	err = c.dealerSocket.SendFrame(serializedMessage, goczmq.FlagNone)
	if err != nil {
		log.Printf("Error sending: %v\n", err)
	}
}

func (c *czmqSocketClient) disconnectedChannel() chan bool {
	return c.disconnectedFromMaster
}
