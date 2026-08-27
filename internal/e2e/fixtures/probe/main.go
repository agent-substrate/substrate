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
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// The actorMetadata data-source files of the systemInfo volume that
// probe.yaml.tmpl mounts at /run/ate.
const (
	identityFile = "/run/ate/actor-id"
	atespaceFile = "/run/ate/atespace"
	uidFile      = "/run/ate/actor-uid"
)

// procStatus is where the kernel reports this process's capability sets. Asking
// the kernel — rather than reading back the OCI spec atelet wrote — is the whole
// point: it is what proves the sandbox actually applied the requested set.
const procStatus = "/proc/self/status"

// capabilityNames maps a capability's bit position to its name, unprefixed to
// match how an ActorTemplate spells them. Indexed by value, so order is fixed
// by the kernel's <linux/capability.h> and must not be sorted. A bit with no
// name here is reported as "CAP_<n>" rather than dropped, so a kernel newer
// than this table still yields a readable diff instead of a silent omission.
var capabilityNames = []string{
	"CHOWN", "DAC_OVERRIDE", "DAC_READ_SEARCH", "FOWNER", "FSETID",
	"KILL", "SETGID", "SETUID", "SETPCAP", "LINUX_IMMUTABLE",
	"NET_BIND_SERVICE", "NET_BROADCAST", "NET_ADMIN", "NET_RAW", "IPC_LOCK",
	"IPC_OWNER", "SYS_MODULE", "SYS_RAWIO", "SYS_CHROOT", "SYS_PTRACE",
	"SYS_PACCT", "SYS_ADMIN", "SYS_BOOT", "SYS_NICE", "SYS_RESOURCE",
	"SYS_TIME", "SYS_TTY_CONFIG", "MKNOD", "LEASE", "AUDIT_WRITE",
	"AUDIT_CONTROL", "SETFCAP", "MAC_OVERRIDE", "MAC_ADMIN", "SYSLOG",
	"WAKE_ALARM", "BLOCK_SUSPEND", "AUDIT_READ", "PERFMON", "BPF",
	"CHECKPOINT_RESTORE",
}

// capabilitiesResponse reports each of the process's capability sets by name.
// Sets are returned in bit order, which is stable, so a test can compare
// against an expected slice without sorting.
type capabilitiesResponse struct {
	Bounding    []string `json:"bounding"`
	Effective   []string `json:"effective"`
	Permitted   []string `json:"permitted"`
	Inheritable []string `json:"inheritable"`
	Ambient     []string `json:"ambient"`
	// Error carries a read/parse failure so a failing assertion explains itself
	// instead of just showing empty sets.
	Error string `json:"error,omitempty"`
}

// decodeCapMask turns a /proc/self/status Cap* hex mask into capability names.
func decodeCapMask(hex string) ([]string, error) {
	mask, err := strconv.ParseUint(strings.TrimSpace(hex), 16, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing capability mask %q: %w", hex, err)
	}
	// Non-nil empty rather than nil: "no capabilities" is a real, assertable
	// result here, and JSON-encoding nil as null muddies that.
	names := []string{}
	for bit := range 64 {
		if mask&(1<<uint(bit)) == 0 {
			continue
		}
		if bit < len(capabilityNames) {
			names = append(names, capabilityNames[bit])
		} else {
			names = append(names, fmt.Sprintf("CAP_%d", bit))
		}
	}
	return names, nil
}

// readProcCapabilities parses the CapInh/CapPrm/CapEff/CapBnd/CapAmb lines.
func readProcCapabilities() (*capabilitiesResponse, error) {
	f, err := os.Open(procStatus)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", procStatus, err)
	}
	defer f.Close()

	out := &capabilitiesResponse{}
	targets := map[string]*[]string{
		"CapInh:": &out.Inheritable,
		"CapPrm:": &out.Permitted,
		"CapEff:": &out.Effective,
		"CapBnd:": &out.Bounding,
		"CapAmb:": &out.Ambient,
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		key, value, found := strings.Cut(scanner.Text(), "\t")
		if !found {
			continue
		}
		dst, ok := targets[strings.TrimSpace(key)]
		if !ok {
			continue
		}
		names, err := decodeCapMask(value)
		if err != nil {
			return nil, err
		}
		*dst = names
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading %s: %w", procStatus, err)
	}
	return out, nil
}

// capabilities reports the capability sets the kernel sees for this process, so
// e2e can assert that an ActorTemplate's securityContext.capabilities actually
// took effect inside the sandbox.
func capabilities(w http.ResponseWriter, _ *http.Request) {
	resp, err := readProcCapabilities()
	if err != nil {
		writeJSON(w, capabilitiesResponse{Error: err.Error()})
		return
	}
	writeJSON(w, resp)
}

// heldIdentity is identityFile opened at startup and held open for the
// probe's whole life, deliberately violating the read-at-time-of-use
// guidance. It exists so a snapshot taken after startup carries live guest
// file state for a system-info file, and a restore must re-bind it (virtiofsd
// find-paths / gofer re-open by path). whoami reads through it on every
// request; after a restore from a shared golden snapshot the read must
// succeed and yield the restored actor's own id, not the golden's.
var heldIdentity *os.File

// whoami reports the actor's identity as observed at request time from the
// bind-mounted identity file. A read failure is reported in the response
// rather than swallowed, so a failing e2e assertion explains itself.
func whoami(w http.ResponseWriter, _ *http.Request) {
	host, _ := os.Hostname()

	resp := map[string]string{"hostname": host}
	for key, path := range map[string]string{
		"file":     identityFile,
		"atespace": atespaceFile,
		"uid":      uidFile,
	} {
		if b, err := os.ReadFile(path); err == nil {
			resp[key] = string(b)
		} else {
			resp[key] = ""
			// Concatenate: a failed assertion should explain every missing file.
			resp["error"] += err.Error() + "; "
		}
	}

	resp["held"] = ""
	if heldIdentity == nil {
		resp["error"] += "identity file was not open at startup; "
	} else if b, err := readAllAt(heldIdentity); err == nil {
		resp["held"] = string(b)
	} else {
		resp["error"] += "reading held identity fd: " + err.Error() + "; "
	}

	writeJSON(w, resp)
}

// readAllAt reads f's full contents from offset 0 without moving its offset,
// so concurrent requests do not interleave seeks on the shared fd.
func readAllAt(f *os.File) ([]byte, error) {
	buf := make([]byte, 4096)
	n, err := f.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf[:n], nil
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
	// Hold the identity file open before serving: every snapshot of this actor
	// then contains an open guest handle on a system-info file (see
	// heldIdentity).
	if f, err := os.Open(identityFile); err == nil {
		heldIdentity = f
	} else {
		log.Printf("probe: opening %s at startup: %v", identityFile, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/whoami", whoami)
	mux.HandleFunc("/resources", resources)
	mux.HandleFunc("/capabilities", capabilities)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	const addr = ":80"
	log.Printf("probe listening on %s", addr)
	server := &http.Server{Addr: addr, Handler: mux}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("probe server: %v", err)
	}
}
