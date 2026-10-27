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
	"maps"
	"math/bits"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
)

// cpuFeatureTables maps each supported architecture to its CPU feature table
// from cpufeatures_table.go, whose header says where the tables come from
// and what a bit means.
var cpuFeatureTables = map[string]featureTable{
	"amd64": newFeatureTable(amd64CPUFeatures),
	"arm64": newFeatureTable(arm64CPUFeatures),
}

// featureTable is one architecture's CPU feature names by bit, with the
// reverse lookup EncodeCPUFeatures uses and the set of bits that have a
// name, which DecodeCPUFeatures validates against.
type featureTable struct {
	names   map[int]string
	bits    []int // the keys of names, ascending
	index   map[string]int
	defined featureMask
}

// newFeatureTable indexes a table. A bit outside featureMask is a table
// error, so it is a panic rather than a dropped name.
func newFeatureTable(names map[int]string) featureTable {
	t := featureTable{
		names: names,
		bits:  slices.Sorted(maps.Keys(names)),
		index: make(map[string]int, len(names)),
	}
	for bit, name := range names {
		if bit < 0 || bit >= featureMaskWords*64 {
			panic(fmt.Sprintf("hardware: CPU feature %q has bit %d outside a %d-bit featureMask", name, bit, featureMaskWords*64))
		}
		t.defined.set(bit)
		t.index[name] = bit
	}
	return t
}

// featureMaskWords is the width of a cpu_features bitmask in 64-bit words.
// gVisor's amd64 blocks 0-8 use bits up to 261, and 6 words leave room for
// blocks through 11 within SchemaVersionV1.
const featureMaskWords = 6

// featureMask is a decoded cpu_features value: bit i set means the feature at
// bit i of the architecture's table is present. It is a fixed-size array so
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

// firstNotIn is the lowest bit set in m but not in w, or -1 if m is a
// subset of w.
func (m featureMask) firstNotIn(w featureMask) int {
	for i := range m {
		if extra := m[i] &^ w[i]; extra != 0 {
			return i*64 + bits.TrailingZeros64(extra)
		}
	}
	return -1
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
// for arch, in bit order. A set bit with no name is an error: the hardware
// reserves it, so no encoder of this schema could have produced the value.
func DecodeCPUFeatures(arch, value string) ([]string, error) {
	table, ok := cpuFeatureTables[arch]
	if !ok {
		return nil, fmt.Errorf("unsupported architecture %q for cpu_features", arch)
	}
	m, ok := parseFeatureMask(value)
	if !ok {
		return nil, fmt.Errorf("invalid cpu_features value %q", value)
	}
	if bit := m.firstNotIn(table.defined); bit >= 0 {
		return nil, fmt.Errorf("cpu_features value %q sets bit %d, which is not a known %s feature", value, bit, arch)
	}
	var out []string
	for _, bit := range table.bits {
		if m.has(bit) {
			out = append(out, table.names[bit])
		}
	}
	return out, nil
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
