// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/spf13/cobra"
)

// newTCPEchoCmd is a raw TCP origin for the egress suites: it echoes whatever
// it is sent, and with --banner it writes that greeting before reading a byte.
//
// The greeting is what this mode exists for. HTTP and gRPC both have the client
// send the first bytes, so neither notices an egress path that waits for
// downstream data before dialing upstream -- the shape a server-speaks-first
// protocol like SSH breaks on. Whether to greet is fixed at startup rather than
// chosen per connection because the server has to decide before it has read
// anything, which is what speaking first means: a suite covering both orders
// deploys two of these rather than sending two kinds of request.
//
// The banner is a flag rather than a constant here so the suite that asserts on
// it is the same place that supplies it, leaving no second copy to drift.
func newTCPEchoCmd() *cobra.Command {
	var (
		listenAddress string
		banner        string
	)
	cmd := &cobra.Command{
		Use:   "tcpecho",
		Short: "Serve a raw TCP echo origin, optionally greeting each peer first.",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			listener, err := net.Listen("tcp", listenAddress)
			if err != nil {
				return fmt.Errorf("listening on %s: %w", listenAddress, err)
			}
			log.Printf("testserver tcpecho: serving on %s, banner %q", listener.Addr(), banner)
			return serveTCPEcho(listener, banner)
		},
	}
	cmd.Flags().StringVar(&listenAddress, "listen", ":2222", "Address the echo origin listens on.")
	cmd.Flags().StringVar(&banner, "banner", "", "Greeting written on accept, before reading anything. Empty stays silent until spoken to.")
	return cmd
}

// serveTCPEcho accepts until the listener fails, which it treats as terminal: a
// pod that stopped accepting but stayed up would fail a test as though the
// egress path had dropped the connection.
func serveTCPEcho(listener net.Listener, banner string) error {
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("accepting on %s: %w", listener.Addr(), err)
		}
		go echoTCP(conn, banner)
	}
}

// echoTCP greets the peer when banner is non-empty, then echoes until it goes
// away.
func echoTCP(conn net.Conn, banner string) {
	// A stuck peer must not hold a goroutine and a socket forever. Long enough
	// that a tunneled round trip is never the thing that trips it.
	const idleTimeout = 60 * time.Second

	defer conn.Close()

	if banner != "" {
		if err := conn.SetWriteDeadline(time.Now().Add(idleTimeout)); err != nil {
			log.Printf("testserver tcpecho: setting write deadline: %v", err)
			return
		}
		if _, err := io.WriteString(conn, banner); err != nil {
			log.Printf("testserver tcpecho: writing banner: %v", err)
			return
		}
	}

	buffer := make([]byte, 4<<10)
	for {
		if err := conn.SetDeadline(time.Now().Add(idleTimeout)); err != nil {
			return
		}
		n, err := conn.Read(buffer)
		if n > 0 {
			if _, writeErr := conn.Write(buffer[:n]); writeErr != nil {
				return
			}
		}
		if err != nil {
			// A peer that hangs up or falls silent is the normal end of a
			// probe, not something worth a line in the pod's log.
			if !errors.Is(err, io.EOF) && !errors.Is(err, os.ErrDeadlineExceeded) {
				log.Printf("testserver tcpecho: reading from peer: %v", err)
			}
			return
		}
	}
}
