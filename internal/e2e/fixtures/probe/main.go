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

// Command probe is a minimal introspection actor used by the e2e suites. It
// reports what the runtime looks like from inside the actor, so tests can
// assert on real in-actor state rather than the config atelet generates.
//
// Keep each endpoint small and independently assertable. New e2e suites add
// probes here.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
)

// identityFile is the actor-id file inside the identity directory atelet
// bind-mounts at IdentityMountPath.
const identityFile = "/run/ate/actor-id"

// whoami reports the actor's identity as observed at request time from the
// bind-mounted identity file. A read failure is reported in the response
// rather than swallowed, so a failing e2e assertion explains itself.
func whoami(w http.ResponseWriter, _ *http.Request) {
	host, _ := os.Hostname()

	resp := map[string]string{"hostname": host}
	if b, err := os.ReadFile(identityFile); err == nil {
		resp["file"] = string(b)
	} else {
		resp["file"] = ""
		resp["error"] = err.Error()
	}

	writeJSON(w, resp)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("probe: encoding response: %v", err)
	}
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/whoami", whoami)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	const addr = ":80"
	log.Printf("probe listening on %s", addr)
	server := &http.Server{Addr: addr, Handler: mux}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("probe server: %v", err)
	}
}
