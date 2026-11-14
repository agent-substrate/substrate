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

package lifecyclehook

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

const (
	defaultMaxWaitDuration       = 100 * time.Millisecond
	maxAllowedWaitDuration       = 250 * time.Millisecond
	maxHooks                     = 8
	maxAllowedAnnotationPrefixes = 16
	maxNameLength                = 63
	maxAddressLength             = 512
)

type HookClientAuthMode string

const (
	HookClientAuthModeMTLS    HookClientAuthMode = "MTLS"
	HookClientAuthModeTLSOnly HookClientAuthMode = "TLSOnly"
)

var (
	dns1123LabelRe = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	addressRe      = regexp.MustCompile(`^(tcp|dns|passthrough|unix)://.+$`)
)

// Config holds the lifecycle hooks configuration loaded from YAML.
// Both bare spec files (top-level maxWaitDuration/hooks) and full
// LifecycleHookConfig manifests (with a nested spec field) are accepted.
type Config struct {
	APIVersion      string         `json:"apiVersion,omitempty"`
	Kind            string         `json:"kind,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
	Spec            *ConfigSpec    `json:"spec,omitempty"`
	MaxWaitDuration string         `json:"maxWaitDuration,omitempty"`
	Hooks           []HookTarget   `json:"hooks,omitempty"`

	waitDuration time.Duration
}

type ConfigSpec struct {
	MaxWaitDuration string       `json:"maxWaitDuration,omitempty"`
	Hooks           []HookTarget `json:"hooks"`
}

// HookTarget is one hook endpoint. A target with no AllowedAnnotationPrefixes
// can allow or deny a create but cannot mutate annotations.
type HookTarget struct {
	Name                      string        `json:"name"`
	Address                   string        `json:"address"`
	AllowedAnnotationPrefixes []string      `json:"allowedAnnotationPrefixes,omitempty"`
	TLS                       HookTLSConfig `json:"tls"`
}

type HookTLSConfig struct {
	CABundle             []byte             `json:"caBundle,omitempty"`
	CACertFile           string             `json:"caCertFile,omitempty"`
	ServerName           string             `json:"serverName"`
	ClientAuthMode       HookClientAuthMode `json:"clientAuthMode,omitempty"`
	ClientCredBundleFile string             `json:"clientCredBundleFile,omitempty"`
}

// LoadConfig reads and validates a lifecycle hooks YAML file from path.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading lifecycle hooks config %q: %w", path, err)
	}
	return ParseConfig(data)
}

// ParseConfig unmarshals and validates YAML configuration bytes.
func ParseConfig(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing lifecycle hooks config: %w", err)
	}
	if err := cfg.NormalizeAndValidate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// WaitDuration returns the validated maximum wait duration for hooks.
func (c *Config) WaitDuration() time.Duration {
	if c == nil || c.waitDuration <= 0 {
		return defaultMaxWaitDuration
	}
	return c.waitDuration
}

type prefixEntry struct {
	hookName string
	prefix   string
}

// NormalizeAndValidate applies defaults and validates all configuration invariants.
func (c *Config) NormalizeAndValidate() error {
	if c == nil {
		return fmt.Errorf("lifecycle hooks config is nil")
	}
	if c.Spec != nil {
		if len(c.Hooks) > 0 || c.MaxWaitDuration != "" {
			return fmt.Errorf("spec cannot be combined with top-level hooks or maxWaitDuration")
		}
		c.MaxWaitDuration = c.Spec.MaxWaitDuration
		c.Hooks = c.Spec.Hooks
		c.Spec = nil
	}
	if c.MaxWaitDuration == "" {
		c.waitDuration = defaultMaxWaitDuration
	} else {
		d, err := time.ParseDuration(c.MaxWaitDuration)
		if err != nil {
			return fmt.Errorf("invalid maxWaitDuration %q: %w", c.MaxWaitDuration, err)
		}
		if d <= 0 || d > maxAllowedWaitDuration {
			return fmt.Errorf("maxWaitDuration %v must be in (0s, %v]", d, maxAllowedWaitDuration)
		}
		c.waitDuration = d
	}

	if len(c.Hooks) == 0 || len(c.Hooks) > maxHooks {
		return fmt.Errorf("hooks must contain between 1 and %d entries, got %d", maxHooks, len(c.Hooks))
	}

	seenNames := make(map[string]bool, len(c.Hooks))
	var seenPrefixes []prefixEntry

	for i := range c.Hooks {
		h := &c.Hooks[i]
		if h.Name == "" || len(h.Name) > maxNameLength || !dns1123LabelRe.MatchString(h.Name) {
			return fmt.Errorf("hooks[%d].name %q must be a valid DNS-1123 label", i, h.Name)
		}
		if seenNames[h.Name] {
			return fmt.Errorf("duplicate hook name %q", h.Name)
		}
		seenNames[h.Name] = true

		if h.Address == "" || len(h.Address) > maxAddressLength || !addressRe.MatchString(h.Address) {
			return fmt.Errorf("hook %q: address %q must use a (tcp|dns|passthrough|unix):// scheme", h.Name, h.Address)
		}

		if len(h.AllowedAnnotationPrefixes) > maxAllowedAnnotationPrefixes {
			return fmt.Errorf("hook %q: allowedAnnotationPrefixes exceeds max %d entries", h.Name, maxAllowedAnnotationPrefixes)
		}
		for _, p := range h.AllowedAnnotationPrefixes {
			if p == "" {
				return fmt.Errorf("hook %q: allowedAnnotationPrefixes must not contain empty strings", h.Name)
			}
			domain, _, _ := strings.Cut(p, "/")
			if domain == "ate.dev" || strings.HasSuffix(domain, ".ate.dev") || strings.HasPrefix(p, "ate.dev/") || strings.Contains(p, ".ate.dev/") {
				return fmt.Errorf("hook %q: annotation prefix %q uses reserved ate.dev domain", h.Name, p)
			}
			for _, prev := range seenPrefixes {
				if strings.HasPrefix(p, prev.prefix) || strings.HasPrefix(prev.prefix, p) {
					return fmt.Errorf("hook %q: annotation prefix %q overlaps with prefix %q on hook %q", h.Name, p, prev.prefix, prev.hookName)
				}
			}
			seenPrefixes = append(seenPrefixes, prefixEntry{hookName: h.Name, prefix: p})
		}

		if err := validateTLSConfig(h.Name, &h.TLS); err != nil {
			return err
		}
	}

	return nil
}

func validateTLSConfig(hookName string, t *HookTLSConfig) error {
	hasCABundle := len(t.CABundle) > 0
	hasCACertFile := strings.TrimSpace(t.CACertFile) != ""
	if hasCABundle == hasCACertFile {
		return fmt.Errorf("hook %q: exactly one of caBundle or caCertFile must be specified", hookName)
	}
	t.ServerName = strings.TrimSpace(t.ServerName)
	if t.ServerName == "" {
		return fmt.Errorf("hook %q: serverName is required", hookName)
	}
	if t.ClientAuthMode == "" {
		t.ClientAuthMode = HookClientAuthModeMTLS
	}
	switch t.ClientAuthMode {
	case HookClientAuthModeMTLS, HookClientAuthModeTLSOnly:
	default:
		return fmt.Errorf("hook %q: invalid clientAuthMode %q", hookName, t.ClientAuthMode)
	}
	if t.ClientAuthMode == HookClientAuthModeTLSOnly && t.ClientCredBundleFile != "" {
		return fmt.Errorf("hook %q: clientCredBundleFile cannot be set when clientAuthMode is TLSOnly", hookName)
	}
	return nil
}
