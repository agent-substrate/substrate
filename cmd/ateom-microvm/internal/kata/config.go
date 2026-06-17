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

package kata

import (
	"fmt"
	"regexp"
)

// ConfigAssets are the runtime-fetched asset paths to splice into a kata
// configuration.toml so the worker image needs no baked /opt/kata. Each is an
// absolute on-node path (content-addressed under the static-files dir, like runsc).
type ConfigAssets struct {
	// Kernel is the guest kernel (vmlinux) path.
	Kernel string
	// Image is the guest OS rootfs image path.
	Image string
	// Hypervisor is the cloud-hypervisor binary path.
	Hypervisor string
	// Virtiofsd is the virtiofsd binary path (needs find-paths migration
	// support, >= 1.13; kata bundles an older build without it).
	Virtiofsd string
}

// configField is a single TOML key whose value we rewrite to a fetched path.
type configField struct {
	key   string
	value func(ConfigAssets) string
}

// pathFields are the asset-path keys in a clh configuration.toml. We rewrite
// each (and its valid_* allowlist) to the fetched path. Empirically validated:
// a stock config with only these lines rewritten boots a VM via KATA_CONF_FILE.
var pathFields = []configField{
	{"kernel", func(a ConfigAssets) string { return quote(a.Kernel) }},
	{"image", func(a ConfigAssets) string { return quote(a.Image) }},
	{"path", func(a ConfigAssets) string { return quote(a.Hypervisor) }},
	{"valid_hypervisor_paths", func(a ConfigAssets) string { return list(a.Hypervisor) }},
	{"virtio_fs_daemon", func(a ConfigAssets) string { return quote(a.Virtiofsd) }},
	{"valid_virtio_fs_daemon_paths", func(a ConfigAssets) string { return list(a.Virtiofsd) }},
}

func quote(s string) string { return `"` + s + `"` }
func list(s string) string  { return `["` + s + `"]` }

// EnableDebug turns on kata's debug knobs in a configuration.toml: it uncomments
// every `#enable_debug = true` (hypervisor/agent/runtime) and appends
// `agent.log=debug` to the hypervisor kernel_params so the guest kata-agent emits
// debug-level logs (with the failing path on errors) over its vsock log channel,
// which the shim relays to our log fifo -> pod logs. POC diagnostic aid.
func EnableDebug(base []byte) []byte {
	out := base
	// Uncomment all `#enable_debug = true` lines.
	reDbg := regexp.MustCompile(`(?m)^(\s*)#\s*enable_debug\s*=\s*true\s*$`)
	out = reDbg.ReplaceAll(out, []byte("${1}enable_debug = true"))
	// Append agent.log=debug to kernel_params (only if not already present).
	reKP := regexp.MustCompile(`(?m)^(\s*kernel_params\s*=\s*")([^"]*)(".*)$`)
	out = reKP.ReplaceAllFunc(out, func(line []byte) []byte {
		m := reKP.FindSubmatch(line)
		if m == nil {
			return line
		}
		existing := string(m[2])
		if regexpContains(existing, "agent.log=") {
			return line
		}
		val := "agent.log=debug agent.debug_console"
		if existing != "" {
			val = existing + " " + val
		}
		return []byte(string(m[1]) + val + string(m[3]))
	})
	return out
}

func regexpContains(s, sub string) bool {
	return regexp.MustCompile(regexp.QuoteMeta(sub)).MatchString(s)
}

// RenderConfig returns base (a kata configuration.toml) with the asset-path
// fields rewritten to point at a.* . The base config carries all the
// version-matched kata settings; we only override where the assets live, so the
// config stays in sync with the kata release (the base itself is a fetched asset).
//
// Each field must already be present in base (kata's stock clh config has them);
// a missing field is an error so we fail loudly rather than boot a half-configured VM.
func RenderConfig(base []byte, a ConfigAssets) ([]byte, error) {
	if a.Kernel == "" || a.Image == "" || a.Hypervisor == "" || a.Virtiofsd == "" {
		return nil, fmt.Errorf("RenderConfig: all of Kernel/Image/Hypervisor/Virtiofsd are required, got %+v", a)
	}
	out := base
	for _, f := range pathFields {
		// Match a top-level `key = <value>` line (TOML), preserving leading
		// whitespace. Anchored to line start in multiline mode.
		re := regexp.MustCompile(`(?m)^(\s*` + regexp.QuoteMeta(f.key) + `\s*=\s*).*$`)
		if !re.Match(out) {
			return nil, fmt.Errorf("RenderConfig: key %q not found in base config", f.key)
		}
		out = re.ReplaceAll(out, []byte("${1}"+f.value(a)))
	}
	return out, nil
}
