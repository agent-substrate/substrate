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
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/spf13/cobra"
)

const udpEchoMaxDatagram = 1500

func serveUDPEcho(conn net.PacketConn) error {
	buf := make([]byte, udpEchoMaxDatagram)
	for {
		n, from, err := conn.ReadFrom(buf)
		if err != nil {
			return err
		}
		if _, err := conn.WriteTo(buf[:n], from); err != nil {
			log.Printf("testserver udpecho: replying to %s: %v", from, err)
		}
	}
}

// newUDPEchoCmd echoes UDP datagrams and serves /healthz over TCP on the same
// port number for kubelet readiness probes.
func newUDPEchoCmd() *cobra.Command {
	var listenAddress string
	cmd := &cobra.Command{
		Use:   "udpecho",
		Short: "Echo UDP datagrams, and answer /healthz on the same port over TCP.",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			conn, err := net.ListenPacket("udp", listenAddress)
			if err != nil {
				return fmt.Errorf("listening for UDP on %s: %w", listenAddress, err)
			}
			defer conn.Close()
			go func() {
				log.Printf("testserver udpecho: echoing UDP on %s", listenAddress)
				log.Printf("testserver udpecho: UDP echo stopped: %v", serveUDPEcho(conn))
			}()

			mux := http.NewServeMux()
			mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			server := &http.Server{
				Addr:              listenAddress,
				Handler:           mux,
				ReadHeaderTimeout: 10 * time.Second,
				WriteTimeout:      2 * time.Minute,
			}
			log.Printf("testserver udpecho: serving readiness on %s", listenAddress)
			return server.ListenAndServe()
		},
	}
	cmd.Flags().StringVar(&listenAddress, "listen", ":8053", "Address the UDP echo and its HTTP readiness endpoint listen on.")
	return cmd
}
