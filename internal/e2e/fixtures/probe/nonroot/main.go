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

// Command nonroot is the probe for an image with a non-root USER and a
// WORKDIR that user owns. It reports the identity, effective capabilities and
// working directory the actor's process runs with.
//
// It is a separate binary from the probe because ko picks the base image per
// import path. The probe stays on the root base, and this one builds on the
// default nonroot distroless base, whose WORKDIR is /home/nonroot.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
)

// identityResponse is the /identity payload.
type identityResponse struct {
	UID    int    `json:"uid"`
	GID    int    `json:"gid"`
	Groups []int  `json:"groups"`
	Cwd    string `json:"cwd"`
	// CapEff is the CapEff mask from /proc/self/status, in hex.
	CapEff string `json:"capEff"`
	// WriteError is why creating a file in the working directory failed.
	WriteError string `json:"writeError,omitempty"`
	Error      string `json:"error,omitempty"`
}

func identity(w http.ResponseWriter, _ *http.Request) {
	resp := identityResponse{UID: os.Getuid(), GID: os.Getgid(), Groups: []int{}}
	if groups, err := os.Getgroups(); err == nil {
		resp.Groups = append(resp.Groups, groups...)
	} else {
		resp.Error += "getgroups: " + err.Error() + "; "
	}
	if cwd, err := os.Getwd(); err == nil {
		resp.Cwd = cwd
	} else {
		resp.Error += "getwd: " + err.Error() + "; "
	}
	if status, err := os.ReadFile("/proc/self/status"); err == nil {
		for line := range strings.Lines(string(status)) {
			if v, ok := strings.CutPrefix(line, "CapEff:"); ok {
				resp.CapEff = strings.TrimSpace(v)
			}
		}
	} else {
		resp.Error += "reading /proc/self/status: " + err.Error() + "; "
	}
	if f, err := os.CreateTemp(".", "write-check-"); err == nil {
		_ = f.Close()
		if err := os.Remove(f.Name()); err != nil {
			resp.WriteError = err.Error()
		}
	} else {
		resp.WriteError = err.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("nonroot probe: writing response: %v", err)
	}
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/identity", identity)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	const addr = ":80"
	log.Printf("nonroot probe listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("nonroot probe server: %v", err)
	}
}
