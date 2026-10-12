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

package credentialprovider

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	kubeletconfigv1 "k8s.io/kubelet/config/v1"
	credentialproviderv1 "k8s.io/kubelet/pkg/apis/credentialprovider/v1"
	"sigs.k8s.io/yaml"
)

// supportedAPIVersion is the only CredentialProviderRequest/Response encoding
// this package speaks. A provider configured for anything else is rejected
// rather than silently sent a request it cannot parse.
var supportedAPIVersion = credentialproviderv1.SchemeGroupVersion.String()

// configKind is the apiVersion/kind a credential provider config file must
// declare.
const (
	configAPIVersion = "kubelet.config.k8s.io/v1"
	configKind       = "CredentialProviderConfig"
)

// loadConfig reads a kubelet CredentialProviderConfig and resolves each
// provider's executable inside binDir, applying the kubelet's per-provider
// validation.
//
// It errors only on the config as a whole (missing, unparseable, wrong kind),
// which means atelet was pointed at the wrong file. Unusable providers are
// skipped, so the result may be short or empty.
func loadConfig(configPath, binDir string) ([]*plugin, error) {
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("while reading credential provider config: %w", err)
	}
	var cfg kubeletconfigv1.CredentialProviderConfig
	if err := yaml.UnmarshalStrict(raw, &cfg); err != nil {
		return nil, fmt.Errorf("while parsing credential provider config %q: %w", configPath, err)
	}
	if cfg.Kind != configKind || cfg.APIVersion != configAPIVersion {
		return nil, fmt.Errorf("credential provider config %q must be %s %s, got %q %q", configPath, configAPIVersion, configKind, cfg.APIVersion, cfg.Kind)
	}
	// Skip rather than fail: the config belongs to the node and may name
	// providers atelet does not implement (tokenAttributes), and one such entry
	// must not cost the node its atelet or its other providers. A pull that
	// needed a skipped provider fails with the registry's own 401.
	seen := make(map[string]struct{}, len(cfg.Providers))
	plugins := make([]*plugin, 0, len(cfg.Providers))
	for i := range cfg.Providers {
		p, err := newPlugin(&cfg.Providers[i], binDir)
		if err != nil {
			slog.Warn("Skipping unusable image credential provider",
				slog.String("config", configPath), slog.Any("err", err))
			continue
		}
		if _, dup := seen[p.name]; dup {
			slog.Warn("Skipping duplicate image credential provider",
				slog.String("config", configPath), slog.String("provider", p.name))
			continue
		}
		seen[p.name] = struct{}{}
		plugins = append(plugins, p)
	}
	// Also not fatal: an empty keychain makes every pull anonymous, which is
	// correct for public registries and fails loudly for private ones.
	if len(plugins) == 0 {
		slog.Error("No usable image credential providers; image pulls will be anonymous",
			slog.String("config", configPath))
	}
	return plugins, nil
}

// newPlugin validates one provider entry and binds it to its executable.
func newPlugin(cfg *kubeletconfigv1.CredentialProvider, binDir string) (*plugin, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("provider name is required")
	}
	// Joined onto binDir, so it must be a bare file name -- "../../bin/sh"
	// would escape the mounted dir.
	if strings.ContainsRune(cfg.Name, filepath.Separator) {
		return nil, fmt.Errorf("provider name %q must not contain %q", cfg.Name, string(filepath.Separator))
	}
	if len(cfg.MatchImages) == 0 {
		return nil, fmt.Errorf("provider %q: matchImages is required", cfg.Name)
	}
	for _, m := range cfg.MatchImages {
		if _, err := matchesImage(m, "example.registry.io/image"); err != nil {
			return nil, fmt.Errorf("provider %q: invalid matchImages entry: %w", cfg.Name, err)
		}
	}
	if cfg.DefaultCacheDuration == nil {
		return nil, fmt.Errorf("provider %q: defaultCacheDuration is required", cfg.Name)
	}
	if cfg.DefaultCacheDuration.Duration < 0 {
		return nil, fmt.Errorf("provider %q: defaultCacheDuration must not be negative", cfg.Name)
	}
	if cfg.APIVersion != supportedAPIVersion {
		return nil, fmt.Errorf("provider %q: apiVersion %q is not supported (want %q)", cfg.Name, cfg.APIVersion, supportedAPIVersion)
	}
	// Unsupported: the kubelet mints a TokenRequest per resolution for the
	// pulling pod's service account, and actors have no Kubernetes service
	// account to mint for.
	if cfg.TokenAttributes != nil {
		return nil, fmt.Errorf("provider %q: tokenAttributes is not supported", cfg.Name)
	}

	env := make([]string, 0, len(cfg.Env))
	for _, e := range cfg.Env {
		env = append(env, e.Name+"="+e.Value)
	}

	return &plugin{
		name:                 cfg.Name,
		path:                 filepath.Join(binDir, cfg.Name),
		args:                 cfg.Args,
		env:                  env,
		matchImages:          cfg.MatchImages,
		defaultCacheDuration: cfg.DefaultCacheDuration.Duration,
		cache:                map[string]cacheEntry{},
	}, nil
}
