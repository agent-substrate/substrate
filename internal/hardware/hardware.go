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

// Package hardware probes host hardware identity and evaluates snapshot
// hardware compatibility between workers and snapshots.
package hardware

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"runtime"
	"slices"
	"strings"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	// AttrArchitecture is the CPU architecture ("amd64", "arm64", etc.).
	AttrArchitecture = "architecture"

	// AttrCPUFeatures is the 64-bit FNV-1a hex digest of the canonical sorted
	// host CPU feature flags from /proc/cpuinfo.
	//
	// The host set is hashed rather than `runsc cpu-features` because runsc is
	// a per-SandboxConfig asset absent at registration, and one worker runs
	// several runsc versions at once.
	AttrCPUFeatures = "cpu_features"

	// AttrCPUVendor is the host CPU vendor identifier (e.g. "GenuineIntel" or
	// "AuthenticAMD" on amd64, or the hex CPU implementer on arm64).
	AttrCPUVendor = "cpu_vendor"

	// AttrCPUModel is the host CPU family and model identifier (e.g. "6/143" on
	// amd64, or "0x1/0xd40" variant/part on arm64). Stepping is intentionally
	// omitted because map attributes use exact equality; if stepping is needed
	// later, it should be introduced as its own attribute key.
	AttrCPUModel = "cpu_model"

	procCPUInfoPath = "/proc/cpuinfo"
)

// hostInfo holds the host architecture and the key-value properties read from
// /proc/cpuinfo in a single pass.
type hostInfo struct {
	arch    string
	cpuinfo map[string]string
}

// lookup returns the value for the first key present in cpuinfo.
func (h hostInfo) lookup(keys ...string) string {
	for _, k := range keys {
		if v := h.cpuinfo[k]; v != "" {
			return v
		}
	}
	return ""
}

// attribute derives a single HardwareIdentity key-value entry from hostInfo.
// Returning ok=false omits the key from the resulting map.
type attribute struct {
	key    string
	derive func(hostInfo) (value string, ok bool)
}

// Profile defines the set of hardware attributes reported by a sandbox runtime.
type Profile struct {
	attrs []attribute
}

// Keys returns the attribute keys declared by the profile in declaration order.
func (p Profile) Keys() []string {
	keys := make([]string, len(p.attrs))
	for i, attr := range p.attrs {
		keys[i] = attr.key
	}
	return keys
}

var (
	attrArchitecture = attribute{
		key:    AttrArchitecture,
		derive: func(h hostInfo) (string, bool) { return h.arch, h.arch != "" },
	}
	attrCPUVendor = attribute{
		key: AttrCPUVendor,
		derive: func(h hostInfo) (string, bool) {
			v := h.lookup("vendor_id", "CPU implementer")
			return v, v != ""
		},
	}
	attrCPUModel = attribute{
		key:    AttrCPUModel,
		derive: deriveModel,
	}
	attrCPUFeatures = attribute{
		key:    AttrCPUFeatures,
		derive: deriveFeatureHash,
	}

	// allAttributes enumerates every defined hardware attribute descriptor.
	allAttributes = []attribute{attrArchitecture, attrCPUVendor, attrCPUModel, attrCPUFeatures}

	// GVisor is the attribute profile reported by ateom-gvisor workers.
	GVisor = Profile{attrs: []attribute{attrArchitecture, attrCPUFeatures}}

	// MicroVM is the attribute profile reported by ateom-microvm workers.
	MicroVM = Profile{attrs: []attribute{attrArchitecture, attrCPUVendor, attrCPUModel}}
)

// Probe inspects the current host and derives the HardwareIdentity for the
// given runtime attribute profile.
func Probe(p Profile) *ateletpb.HardwareIdentity {
	return identity(readHost(procCPUInfoPath, runtime.GOARCH), p)
}

func identity(h hostInfo, p Profile) *ateletpb.HardwareIdentity {
	attrs := make(map[string]string, len(p.attrs))
	for _, attr := range p.attrs {
		if val, ok := attr.derive(h); ok {
			attrs[attr.key] = val
		}
	}
	return &ateletpb.HardwareIdentity{Attributes: attrs}
}

func readHost(cpuInfoPath, arch string) hostInfo {
	h := hostInfo{arch: arch}
	if f, err := os.Open(cpuInfoPath); err == nil {
		defer f.Close()
		h.cpuinfo = parseCPUInfo(f)
	}
	return h
}

// parseCPUInfo reads key-value pairs from /proc/cpuinfo. It keeps the first
// occurrence of each key, assuming homogeneous cores across the host VM.
func parseCPUInfo(r io.Reader) map[string]string {
	fields := make(map[string]string)
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		key, val, ok := strings.Cut(scanner.Text(), ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		if key == "" || val == "" {
			continue
		}
		if _, exists := fields[key]; !exists {
			fields[key] = val
		}
	}
	return fields
}

func deriveModel(h hostInfo) (string, bool) {
	switch h.arch {
	case "amd64":
		family, model := h.lookup("cpu family"), h.lookup("model")
		if family != "" && model != "" {
			return family + "/" + model, true
		}
		return "", false
	case "arm64":
		variant, part := h.lookup("CPU variant"), h.lookup("CPU part")
		if variant != "" && part != "" {
			return variant + "/" + part, true
		}
		return part, part != ""
	default:
		return "", false
	}
}

func deriveFeatureHash(h hostInfo) (string, bool) {
	flags := strings.Fields(h.lookup("flags", "Features"))
	if len(flags) == 0 {
		return "", false
	}
	slices.Sort(flags)
	flags = slices.Compact(flags)
	hash := fnv.New64a()
	for i, f := range flags {
		if i > 0 {
			_, _ = hash.Write([]byte{' '})
		}
		_, _ = hash.Write([]byte(f))
	}
	return fmt.Sprintf("%016x", hash.Sum64()), true
}

// Matches reports whether worker satisfies the hardware identity recorded on
// snap. A nil or empty snapshot HardwareIdentity imposes no constraint.
func Matches(worker, snap *ateapipb.HardwareIdentity) bool {
	for k, v := range snap.GetAttributes() {
		if worker.GetAttributes()[k] != v {
			return false
		}
	}
	return true
}
