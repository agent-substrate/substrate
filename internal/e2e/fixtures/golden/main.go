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

// Command golden either exits immediately or serves a process-local boot ID.
// Restoring two actors from one golden must preserve each container's boot ID;
// a cold boot generates a different one.
package main

import (
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

func main() {
	exitCode := flag.Int("exit-code", -1, "exit immediately with this status; -1 serves HTTP")
	port := flag.Int("port", 8080, "HTTP port")
	flag.Parse()
	if *exitCode >= 0 {
		os.Exit(*exitCode)
	}

	bootID := rand.Text()
	var counter atomic.Int64
	mux := http.NewServeMux()
	writeState := func(w http.ResponseWriter, count int64) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(struct {
			BootID  string `json:"bootID"`
			Counter int64  `json:"counter"`
		}{bootID, count}); err != nil {
			log.Printf("write application state: %v", err)
		}
	}
	mux.HandleFunc("GET /state", func(w http.ResponseWriter, _ *http.Request) {
		writeState(w, counter.Load())
	})
	mux.HandleFunc("POST /increment", func(w http.ResponseWriter, _ *http.Request) {
		writeState(w, counter.Add(1))
	})
	mux.HandleFunc("POST /exit", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusAccepted)
		if err := http.NewResponseController(w).Flush(); err != nil {
			log.Printf("flush exit response: %v", err)
		}
		os.Exit(1)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, bootID)
	})
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", *port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
