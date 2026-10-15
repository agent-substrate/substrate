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
	"fmt"
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

func runtimeState(w http.ResponseWriter, r *http.Request) {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	kind := r.URL.Query().Get("runtime")
	var cmdline []byte
	if kind == "" || kind == "microvm" {
		cmdline, err = os.ReadFile("/proc/cmdline")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}
	marker, err := runtimeBootIdentity(kind, strings.TrimSpace(string(b)), string(cmdline))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, map[string]string{"boot_id": marker, "kernel_boot_id": strings.TrimSpace(string(b)), "process_nonce": processNonce})
}

// gVisor supplies a diagnostic boot ID that changes on restore. MicroVM
// continuity uses our eager marker because Linux initializes boot_id lazily.
func runtimeBootIdentity(kind, kernelBoot, cmdline string) (string, error) {
	if kind == "gvisor" {
		if kernelBoot == "" {
			return "", fmt.Errorf("sandbox boot identity missing")
		}
		return kernelBoot, nil
	}
	if kind != "" && kind != "microvm" {
		return "", fmt.Errorf("unsupported runtime %q", kind)
	}
	for _, arg := range strings.Fields(cmdline) {
		if marker, ok := strings.CutPrefix(arg, "ate.boot_id="); ok && marker != "" {
			return marker, nil
		}
	}
	return "", fmt.Errorf("guest boot marker missing")
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
