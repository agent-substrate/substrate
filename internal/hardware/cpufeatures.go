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

package hardware

import (
	"bufio"
	"fmt"
	"io"
	"math/bits"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
)

// cpuFeatureTables maps each supported architecture to its CPU feature table.
// The tables live in zz_generated.cpufeatures.go, generated from gVisor's
// sources by hack/update/cpufeatures.sh (see its header for how to move to a
// newer gVisor). A name's index is its bit in the encoded cpu_features value,
// and stored snapshots carry those values, so within SchemaVersionV1 the
// generator only appends; a name gVisor drops stays at its index as retired.
var cpuFeatureTables = map[string]featureTable{
	"amd64": newFeatureTable(amd64CPUFeatures),
	"arm64": newFeatureTable(arm64CPUFeatures),
}

// featureTable assigns each CPU feature name of one architecture its bit:
// the name's index in names.
type featureTable struct {
	names []string
	index map[string]int
}

func newFeatureTable(names []string) featureTable {
	index := make(map[string]int, len(names))
	for i, name := range names {
		index[name] = i
	}
	return featureTable{names: names, index: index}
}

// featureMaskWords is the width of a cpu_features bitmask in 64-bit words,
// which bounds how far a table can grow within SchemaVersionV1.
// TestCPUFeatureTables checks each table fits.
const featureMaskWords = 4

// featureMask is a decoded cpu_features value: bit i set means the feature at
// index i of the architecture's table is present. It is a fixed-size array so
// matching on the scheduler's hot path allocates nothing.
type featureMask [featureMaskWords]uint64

func (m *featureMask) set(bit int) {
	m[bit/64] |= 1 << (bit % 64)
}

func (m featureMask) has(bit int) bool {
	return m[bit/64]&(1<<(bit%64)) != 0
}

// subsetOf reports whether every bit set in m is also set in w.
func (m featureMask) subsetOf(w featureMask) bool {
	for i := range m {
		if m[i]&^w[i] != 0 {
			return false
		}
	}
	return true
}

// bitLen is one more than the highest set bit, or 0 for the empty mask.
func (m featureMask) bitLen() int {
	for i := featureMaskWords - 1; i >= 0; i-- {
		if m[i] != 0 {
			return i*64 + bits.Len64(m[i])
		}
	}
	return 0
}

// String is the canonical encoding: lowercase hex, no prefix, no leading
// zeros, "0" for the empty mask. Equal sets encode to equal strings, which
// is what proto equality on a recorded runtime compares.
func (m featureMask) String() string {
	top := featureMaskWords - 1
	for top > 0 && m[top] == 0 {
		top--
	}
	var sb strings.Builder
	sb.WriteString(strconv.FormatUint(m[top], 16))
	for i := top - 1; i >= 0; i-- {
		fmt.Fprintf(&sb, "%016x", m[i])
	}
	return sb.String()
}

// parseFeatureMask parses the canonical encoding and rejects anything else.
func parseFeatureMask(s string) (featureMask, bool) {
	var m featureMask
	if s == "" || len(s) > featureMaskWords*16 || (len(s) > 1 && s[0] == '0') {
		return m, false
	}
	for i := 0; i < len(s); i++ {
		c := s[len(s)-1-i]
		var v uint64
		switch {
		case '0' <= c && c <= '9':
			v = uint64(c - '0')
		case 'a' <= c && c <= 'f':
			v = uint64(c-'a') + 10
		default:
			return featureMask{}, false
		}
		m[i/16] |= v << (4 * (i % 16))
	}
	return m, true
}

// EncodeCPUFeatures encodes the feature names `runsc cpu-features` printed
// for arch as a cpu_features value. Blank names are skipped; duplicates and
// order do not matter. An unknown name is an error rather than a dropped
// bit: a snapshot encoded without a feature its sandbox used would match
// workers that lack it.
func EncodeCPUFeatures(arch string, features []string) (string, error) {
	table, ok := cpuFeatureTables[arch]
	if !ok {
		return "", fmt.Errorf("unsupported architecture %q for cpu_features", arch)
	}
	var m featureMask
	for _, raw := range features {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		bit, known := table.index[name]
		if !known {
			return "", fmt.Errorf("unknown %s CPU feature %q", arch, name)
		}
		m.set(bit)
	}
	return m.String(), nil
}

// DecodeCPUFeatures returns the feature names a cpu_features value encodes
// for arch, in table order.
func DecodeCPUFeatures(arch, value string) ([]string, error) {
	table, ok := cpuFeatureTables[arch]
	if !ok {
		return nil, fmt.Errorf("unsupported architecture %q for cpu_features", arch)
	}
	m, ok := parseFeatureMask(value)
	if !ok {
		return nil, fmt.Errorf("invalid cpu_features value %q", value)
	}
	if n := m.bitLen(); n > len(table.names) {
		return nil, fmt.Errorf("cpu_features value %q has bit %d beyond the %d known %s features", value, n-1, len(table.names), arch)
	}
	var out []string
	for i, name := range table.names {
		if m.has(i) {
			out = append(out, name)
		}
	}
	return out, nil
}

// matchesCPUFeatures reports whether every feature in snapVal is in
// workerVal. A value that is not a canonical mask matches nothing.
func matchesCPUFeatures(workerVal, snapVal string) bool {
	w, ok := parseFeatureMask(workerVal)
	if !ok {
		return false
	}
	s, ok := parseFeatureMask(snapVal)
	if !ok {
		return false
	}
	return s.subsetOf(w)
}

// ProbeCPUIdentity reads /proc/cpuinfo for the host CPU's vendor and model:
// the AttrCPUVendor and AttrCPUModel values of a microVM runtime.
func ProbeCPUIdentity() (vendor, model string, err error) {
	f, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return "", "", fmt.Errorf("while opening /proc/cpuinfo: %w", err)
	}
	defer f.Close()
	return parseCPUInfo(runtime.GOARCH, f)
}

// parseCPUInfo extracts the vendor and model for arch from /proc/cpuinfo
// content, from the first processor that lists them:
//   - amd64: vendor_id (e.g. "GenuineIntel") and "<cpu family>:<model>"
//     (e.g. "6:143").
//   - arm64: CPU implementer (e.g. "0x41") and
//     "<CPU variant>:<CPU part>:<CPU revision>" (e.g. "0x0:0xd40:1").
func parseCPUInfo(arch string, r io.Reader) (vendor, model string, err error) {
	// The vendor key first, then the keys the model is composed of.
	var keys []string
	switch arch {
	case "amd64":
		keys = []string{"vendor_id", "cpu family", "model"}
	case "arm64":
		keys = []string{"CPU implementer", "CPU variant", "CPU part", "CPU revision"}
	default:
		return "", "", fmt.Errorf("unsupported architecture %q for CPU identity", arch)
	}
	fields := make(map[string]string, len(keys))
	scanner := bufio.NewScanner(r)
	for len(fields) < len(keys) && scanner.Scan() {
		k, v, ok := strings.Cut(scanner.Text(), ":")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		if _, seen := fields[k]; !seen && slices.Contains(keys, k) {
			fields[k] = strings.TrimSpace(v)
		}
	}
	if err := scanner.Err(); err != nil {
		return "", "", fmt.Errorf("while reading /proc/cpuinfo: %w", err)
	}
	values := make([]string, 0, len(keys))
	for _, k := range keys {
		v, ok := fields[k]
		if !ok || v == "" {
			return "", "", fmt.Errorf("missing %q in /proc/cpuinfo", k)
		}
		values = append(values, v)
	}
	return values[0], strings.Join(values[1:], ":"), nil
}
