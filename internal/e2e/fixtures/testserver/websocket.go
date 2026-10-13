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
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func newWebsocketHandler(echo bool) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Print("upgrade:", err)
			return
		}
		defer c.Close()
		for {
			mt, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				break
			}
			log.Printf("recv: %s", message)

			var response []byte
			switch {
			case echo:
				response = message
			case string(message) == "PING":
				response = []byte("PONG")
			default:
				continue
			}
			if err := c.WriteMessage(mt, response); err != nil {
				log.Println("write:", err)
				break
			}
		}
	})
	return mux
}

func newWebsocketCmd() *cobra.Command {
	var (
		listenAddress string
		certFile      string
		keyFile       string
		echo          bool
	)
	cmd := &cobra.Command{
		Use:   "websocket",
		Short: "Serve a websocket server for e2e tests.",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			server := &http.Server{
				Addr:    listenAddress,
				Handler: newWebsocketHandler(echo),
			}
			log.Printf("testserver websocket: listening on %s", listenAddress)
			return serveHTTP(server, certFile, keyFile)
		},
	}
	cmd.Flags().StringVar(&listenAddress, "listen", ":80", "Address the websocket server listens on.")
	cmd.Flags().StringVar(&certFile, "tls-cert", "", "PEM certificate file; enables HTTPS when paired with --tls-key.")
	cmd.Flags().StringVar(&keyFile, "tls-key", "", "PEM private key file; enables HTTPS when paired with --tls-cert.")
	cmd.Flags().BoolVar(&echo, "echo", false, "Echo every WebSocket message with its original type and payload.")
	return cmd
}
