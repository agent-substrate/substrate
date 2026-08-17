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

// Command bannerserver is a TCP origin for egress tests. It echoes what it is
// sent on two ports: on one it greets the peer first, on the other it stays
// silent until spoken to.
//
// Which port a test dials is how it selects the behavior. Nothing in the
// request can select it, because the server has to decide whether to greet
// before it has read anything -- that is what speaking first means.
package main

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

// Banner is what the server writes on accept. Tests match on it, so it is a
// fixed string rather than anything derived from the connection.
const Banner = "TESTBANNER/1.0\r\n"

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	greeting := listenerAddress("LISTEN_ADDRESS", ":2222")
	quiet := listenerAddress("QUIET_LISTEN_ADDRESS", ":2223")

	var group sync.WaitGroup
	group.Add(2)
	go func() { defer group.Done(); accept(greeting, true) }()
	go func() { defer group.Done(); accept(quiet, false) }()
	group.Wait()
}

func listenerAddress(variable, fallback string) string {
	if address := os.Getenv(variable); address != "" {
		return address
	}
	return fallback
}

// accept serves address until it stops accepting, greeting each peer first when
// greet is set. A dead listener takes the process down rather than leaving it
// half-serving: a test that reached the surviving port would pass while the
// other silently answered nothing.
func accept(address string, greet bool) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		slog.Error("banner server failed to listen", "address", address, "error", err)
		os.Exit(1)
	}
	slog.Info("starting banner server", "address", address, "greets", greet)

	for {
		connection, err := listener.Accept()
		if err != nil {
			slog.Error("banner server stopped accepting", "address", address, "error", err)
			os.Exit(1)
		}
		go serve(connection, greet)
	}
}

// serve echoes until the peer goes away, announcing itself first when greet is
// set.
func serve(connection net.Conn, greet bool) {
	// A stuck peer must not hold a goroutine and a socket forever. Long enough
	// that a tunneled round trip is never the thing that trips it.
	const idleTimeout = 60 * time.Second

	defer connection.Close()

	if greet {
		if err := connection.SetWriteDeadline(time.Now().Add(idleTimeout)); err != nil {
			slog.Error("setting write deadline", "error", err)
			return
		}
		if _, err := io.WriteString(connection, Banner); err != nil {
			slog.Error("writing banner", "error", err)
			return
		}
	}

	buffer := make([]byte, 4<<10)
	for {
		if err := connection.SetDeadline(time.Now().Add(idleTimeout)); err != nil {
			return
		}
		n, err := connection.Read(buffer)
		if n > 0 {
			if _, writeErr := connection.Write(buffer[:n]); writeErr != nil {
				return
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, os.ErrDeadlineExceeded) {
				slog.Error("reading from peer", "error", err)
			}
			return
		}
	}
}
