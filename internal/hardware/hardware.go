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

// Package hardware probes host hardware compatibility attributes and evaluates
// sandbox runtime compatibility between workers and snapshots.
package hardware

import (
	"bufio"
	"io"
	"os"
	"runtime"
	"strings"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	// SchemaVersionV1 is the initial compatibility schema version.
	SchemaVersionV1 = "v1"

	// AttrArchitecture is the CPU architecture ("amd64", "arm64", etc.).
	AttrArchitecture = "architecture"

	// AttrCPUVendor is the host CPU vendor: the vendor_id string on amd64
	// ("GenuineIntel", "AuthenticAMD") or the hex CPU implementer on arm64.
	AttrCPUVendor = "cpu_vendor"

	// AttrCPUModel is the host CPU family and model, "family/model" on amd64
	// (e.g. "6/143") or "variant/part" on arm64 (e.g. "0x1/0xd40"). Stepping is
	// left out because attributes match by exact equality; if it ever matters
	// it belongs under its own key.
	AttrCPUModel = "cpu_model"

	// TODO: Add AttrCPUFeatures ("cpu_features") for gVisor, which restores a
	// checkpoint only when its CPUID feature set is a subset of the host's.
	// The value should derive from CPUID rather than the /proc/cpuinfo flags
	// line, whose kernel-synthesized entries change with the node kernel.
	// Also add AttrGVisorAssetHash ("gvisor_asset_hash") once ateom knows
	// which runsc it runs.

	procCPUInfoPath = "/proc/cpuinfo"
)

// hostInfo is the host architecture and the key-value fields read from
// /proc/cpuinfo in one pass.
type hostInfo struct {
	arch    string
	cpuinfo map[string]string
}

// lookup returns the value for the first of keys present in cpuinfo.
func (h hostInfo) lookup(keys ...string) string {
	for _, k := range keys {
		if v := h.cpuinfo[k]; v != "" {
			return v
		}
	}
	return ""
}

// attribute derives one compatibility attribute from hostInfo. ok=false
// leaves the key out of the reported identity.
type attribute struct {
	key    string
	derive func(hostInfo) (value string, ok bool)
}

// Profile is the set of attributes a sandbox runtime's restore path depends
// on, and so the set its workers report.
type Profile struct {
	attrs []attribute
}

// Keys returns the profile's attribute keys in declaration order.
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

	// allAttributes is every attribute a profile may draw from.
	allAttributes = []attribute{attrArchitecture, attrCPUVendor, attrCPUModel}

	// Base is what any runtime needs at minimum: a snapshot cannot boot at all,
	// cold or warm, on another architecture.
	Base = Profile{attrs: []attribute{attrArchitecture}}

	// GVisor is reported by ateom-gvisor workers.
	//
	// TODO: Add cpu_features; see the AttrCPUFeatures TODO above.
	GVisor = Profile{attrs: []attribute{attrArchitecture}}

	// MicroVM is reported by ateom-microvm workers. Cloud Hypervisor has no
	// compatibility gate of its own, so vendor and model are the coarse guard
	// against loading vCPU state captured on a different CPU.
	MicroVM = Profile{attrs: []attribute{attrArchitecture, attrCPUVendor, attrCPUModel}}
)

// ProfileFor returns the Profile workers of sandboxClass report. Classes this
// package does not know report Base.
func ProfileFor(sandboxClass string) Profile {
	switch atev1alpha1.SandboxClass(sandboxClass) {
	case atev1alpha1.SandboxClassGvisor:
		return GVisor
	case atev1alpha1.SandboxClassMicroVM:
		return MicroVM
	default:
		return Base
	}
}

// ProbeHost inspects the current host and returns its v1 VersionedSandboxCompat
// for p, in atelet wire format. Attributes the host does not expose are left
// out rather than reported empty.
func ProbeHost(p Profile) *ateletpb.VersionedSandboxCompat {
	return compat(readHost(procCPUInfoPath, runtime.GOARCH), p)
}

func compat(h hostInfo, p Profile) *ateletpb.VersionedSandboxCompat {
	out := &ateletpb.VersionedSandboxCompat{SchemaVersion: SchemaVersionV1}
	for _, attr := range p.attrs {
		if val, ok := attr.derive(h); ok {
			out.Attributes = append(out.Attributes, &ateletpb.AttributeEntry{Key: attr.key, Value: val})
		}
	}
	return out
}

func readHost(cpuInfoPath, arch string) hostInfo {
	h := hostInfo{arch: arch}
	if f, err := os.Open(cpuInfoPath); err == nil {
		defer f.Close()
		h.cpuinfo = parseCPUInfo(f)
	}
	return h
}

// parseCPUInfo reads the key-value lines of /proc/cpuinfo, keeping the first
// occurrence of each key: the cores of a VM are taken to be homogeneous.
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
		if family == "" || model == "" {
			return "", false
		}
		return family + "/" + model, true
	case "arm64":
		variant, part := h.lookup("CPU variant"), h.lookup("CPU part")
		if part == "" {
			return "", false
		}
		if variant == "" {
			return part, true
		}
		return variant + "/" + part, true
	default:
		return "", false
	}
}

// Matches reports whether a worker's SandboxRuntime can restore a snapshot
// stamped with snap. A nil snapshot SandboxRuntime imposes no constraint.
// Otherwise the sandbox classes must be equal and MatchesCompat must hold.
func Matches(worker, snap *ateapipb.SandboxRuntime) bool {
	if snap == nil {
		return true
	}
	if worker == nil || worker.GetSandboxClass() != snap.GetSandboxClass() {
		return false
	}
	return MatchesCompat(worker.GetVersion(), snap.GetVersion())
}

// MatchesCompat reports whether worker can restore a snapshot stamped with
// snap: same non-empty schema version, and every attribute on snap present on
// worker with an equal value. Keys only the worker has are ignored; a new
// schema version is how older snapshots are excluded.
func MatchesCompat(worker, snap *ateapipb.VersionedSandboxCompat) bool {
	if worker == nil || snap == nil {
		return false
	}
	if worker.GetSchemaVersion() == "" || worker.GetSchemaVersion() != snap.GetSchemaVersion() {
		return false
	}
	wAttrs := worker.GetAttributes()
	sAttrs := snap.GetAttributes()
	// API validation bounds attributes to at most 32 entries with unique keys,
	// so the nested scan is small and avoids map allocation on the hot path.
	for _, sa := range sAttrs {
		found := false
		for _, wa := range wAttrs {
			if wa.GetKey() == sa.GetKey() {
				if wa.GetValue() != sa.GetValue() {
					return false
				}
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
