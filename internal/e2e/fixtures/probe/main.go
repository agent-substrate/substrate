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
	"bufio"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
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

// resources reports the compute envelope the actor observes from inside the
// sandbox, so the sizing e2e suite can assert the actor's declared limits
// actually shaped the runtime.
//
//   - num_cpu is runtime.NumCPU(): for the gVisor runtime this is the sentry's
//     vCPU count, provisioned from the CPU limit via runsc --cpu-num-from-quota,
//     so it equals ceil(limits.cpu).
//   - mem_total_bytes is MemTotal from /proc/meminfo: the memory the sandbox
//     believes it has, bounded by limits.memory.
//   - cpu_max / memory_max are the raw cgroup v2 files, reported best-effort for
//     debugging; presence and format vary by runtime.
func resources(w http.ResponseWriter, _ *http.Request) {
	resp := map[string]any{"num_cpu": runtime.NumCPU()}

	if v, err := memTotalBytes(); err == nil {
		resp["mem_total_bytes"] = v
	} else {
		resp["mem_total_error"] = err.Error()
	}
	if b, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
		resp["cpu_max"] = strings.TrimSpace(string(b))
	}
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		resp["memory_max"] = strings.TrimSpace(string(b))
	}

	writeJSON(w, resp)
}

// memTotalBytes parses MemTotal (reported in kB) from /proc/meminfo.
func memTotalBytes() (int64, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		rest, ok := strings.CutPrefix(line, "MemTotal:")
		if !ok {
			continue
		}
		fields := strings.Fields(rest) // e.g. "524288 kB"
		if len(fields) == 0 {
			break
		}
		kb, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return 0, err
		}
		return kb * 1024, nil
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	return 0, os.ErrNotExist
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
	mux.HandleFunc("/resources", resources)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	const addr = ":80"
	log.Printf("probe listening on %s", addr)
	server := &http.Server{Addr: addr, Handler: mux}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("probe server: %v", err)
	}
}
