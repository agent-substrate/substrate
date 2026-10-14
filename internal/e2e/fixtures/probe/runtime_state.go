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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// Generated once per process; unlike boot_id, runtime-only clones must differ.
var processNonce = func() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}()

func runtimeState(w http.ResponseWriter, _ *http.Request) {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cmdline, err := os.ReadFile("/proc/cmdline")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	var marker string
	for _, arg := range strings.Fields(string(cmdline)) {
		if value, ok := strings.CutPrefix(arg, "ate.boot_id="); ok {
			marker = value
		}
	}
	if marker == "" {
		http.Error(w, "guest boot marker missing", 500)
		return
	}
	writeJSON(w, map[string]string{"boot_id": marker, "kernel_boot_id": strings.TrimSpace(string(b)), "process_nonce": processNonce})
}

// seedVolumeFixture seeds a disposable fixture before the retained-state probe is
// primed. Never call it after restore: that would conceal stale volume state.
func seedVolumeFixture(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Root  string `json:"root"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192)).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if !filepath.IsAbs(req.Root) || req.Root == "/" || req.Value == "" {
		http.Error(w, "absolute disposable root and value required", 400)
		return
	}
	if err := os.MkdirAll(filepath.Join(req.Root, "directory"), 0755); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	for _, name := range []string{"cached.txt", "held.txt", "unlinked.txt", "directory/entry"} {
		if err := os.WriteFile(filepath.Join(req.Root, name), []byte(req.Value), 0644); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}
	writeJSON(w, map[string]string{"ok": "true"})
}
