// Copyright 2026 The Agent Substrate Authors
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
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
)

// featureMaskWords is the width of a cpu_features bitmask in 64-bit words.
// gVisor's amd64 blocks 0-8 use bits up to 261, and 6 words leave room for
// blocks through 11 within SchemaVersionV1.
const featureMaskWords = 6

// featureMask is a decoded cpu_features value: bit i set means gVisor's
// Feature i is present. That number is fixed by the hardware on amd64, 32 *
// CPUID block + the feature's bit in that block's register, and declared by
// gVisor on arm64, so a bit never moves between gVisor versions and masks
// from different runsc binaries compare. It is a fixed-size array so matching
// on the scheduler's hot path allocates nothing.
type featureMask [featureMaskWords]uint64

// subsetOf reports whether every bit set in m is also set in w.
func (m featureMask) subsetOf(w featureMask) bool {
	for i := range m {
		if m[i]&^w[i] != 0 {
			return false
		}
	}
	return true
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

// EncodeCPUFeatureMask encodes the blocks `runsc cpu-features --bitmask`
// prints as a cpu_features value: one "0x%08x" uint32 per 32 features,
// lowest first, as many as the binary's newest feature needs, so trailing
// zero blocks change nothing. More blocks than featureMask holds is an error.
func EncodeCPUFeatureMask(blocks string) (string, error) {
	var m featureMask
	for i, block := range strings.Split(strings.TrimSpace(blocks), ",") {
		if i >= featureMaskWords*2 {
			return "", fmt.Errorf("cpu_features bitmask %q has more than %d blocks", blocks, featureMaskWords*2)
		}
		digits, ok := strings.CutPrefix(block, "0x")
		if !ok {
			return "", fmt.Errorf("cpu_features bitmask block %q lacks the 0x prefix", block)
		}
		v, err := strconv.ParseUint(digits, 16, 32)
		if err != nil {
			return "", fmt.Errorf("cpu_features bitmask block %q: %w", block, err)
		}
		m[i/2] |= v << (32 * (i % 2))
	}
	return m.String(), nil
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
