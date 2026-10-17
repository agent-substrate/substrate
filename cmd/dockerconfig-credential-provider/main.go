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

// dockerconfig-credential-provider — a kubelet-protocol image credential provider that answers from a Docker
// config file (the `.dockerconfigjson` of a Kubernetes Secret, mounted read-only), so Agent Substrate's node
// agent (atelet) can pull PRIVATE actor images with ordinary Kubernetes Secrets.
//
// atelet pulls actor images itself and authenticates through kubelet credential-provider exec plugins
// (--image-credential-provider-config / --image-credential-provider-bin-dir). Outside GKE there is usually no
// node plugin for the registry an install pulls from; this one serves static credentials from a Secret, so a
// cluster needs neither host files nor a cloud-specific plugin (see
// manifests/ate-install/components/image-credential-secret).
//
// Protocol (credentialprovider.kubelet.k8s.io/v1): a CredentialProviderRequest {image} as JSON on stdin; a
// CredentialProviderResponse on stdout. The docker config is $DOCKER_CONFIG/config.json (set DOCKER_CONFIG
// in the provider's `env` in the CredentialProviderConfig). No matching registry → an empty `auth` (the pull
// falls back to the next provider / anonymous); a malformed request or config → exit 1.
//
// `dockerconfig-credential-provider install <dir>` copies this binary to <dir>/dockerconfig-credential-provider
// (the image is distroless — no cp) for the init container. Credentials are never logged.
package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	apiVersion    = "credentialprovider.kubelet.k8s.io/v1"
	binaryName    = "dockerconfig-credential-provider"
	cacheDuration = "5m" // atelet ignores durations under one minute (#917)
)

type request struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Image      string `json:"image"`
}

type authConfig struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type response struct {
	APIVersion    string                `json:"apiVersion"`
	Kind          string                `json:"kind"`
	CacheKeyType  string                `json:"cacheKeyType"`
	CacheDuration string                `json:"cacheDuration"`
	Auth          map[string]authConfig `json:"auth"`
}

// dockerConfig is the `.dockerconfigjson` / ~/.docker/config.json shape.
type dockerConfig struct {
	Auths map[string]struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Auth     string `json:"auth"` // base64("user:password"), used when username/password are absent
	} `json:"auths"`
}

func main() {
	if len(os.Args) == 3 && os.Args[1] == "install" {
		if err := install(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, "install:", err)
			os.Exit(1)
		}
		return
	}
	if err := run(os.Stdin, os.Stdout, os.Getenv("DOCKER_CONFIG")); err != nil {
		fmt.Fprintln(os.Stderr, binaryName+":", err)
		os.Exit(1)
	}
}

func run(in io.Reader, out io.Writer, configDir string) error {
	var req request
	if err := json.NewDecoder(in).Decode(&req); err != nil {
		return fmt.Errorf("decode CredentialProviderRequest: %w", err)
	}
	if req.APIVersion != apiVersion || req.Kind != "CredentialProviderRequest" || req.Image == "" {
		return fmt.Errorf("unsupported request (apiVersion %q, kind %q, image set: %t)", req.APIVersion, req.Kind, req.Image != "")
	}
	if configDir == "" {
		return errors.New("DOCKER_CONFIG is not set (point it at the mounted Secret's directory)")
	}
	cfg, err := loadConfig(filepath.Join(configDir, "config.json"))
	if err != nil {
		return err
	}
	resp := response{APIVersion: apiVersion, Kind: "CredentialProviderResponse", CacheKeyType: "Registry",
		CacheDuration: cacheDuration, Auth: map[string]authConfig{}}
	host := registryHost(req.Image)
	if a, ok := lookup(cfg, host); ok {
		resp.Auth[host] = a
	}
	return json.NewEncoder(out).Encode(resp)
}

func loadConfig(path string) (*dockerConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read docker config: %w", err)
	}
	var cfg dockerConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse docker config %s: %w", path, err)
	}
	return &cfg, nil
}

// registryHost returns the registry part of an image reference, the way Docker resolves it: the first path
// component when it looks like a host (has a dot or a port, or is localhost), else Docker Hub.
func registryHost(image string) string {
	first, _, found := strings.Cut(image, "/")
	if found && (strings.ContainsAny(first, ".:") || first == "localhost") {
		return first
	}
	return "docker.io"
}

// lookup finds the entry for host; config keys may be bare hosts or URLs (https://ghcr.io, https://index.docker.io/v1/).
func lookup(cfg *dockerConfig, host string) (authConfig, bool) {
	for key, e := range cfg.Auths {
		k := strings.TrimPrefix(strings.TrimPrefix(key, "https://"), "http://")
		k, _, _ = strings.Cut(k, "/")
		if k == "index.docker.io" {
			k = "docker.io"
		}
		if k != host {
			continue
		}
		user, pass := e.Username, e.Password
		if (user == "" || pass == "") && e.Auth != "" {
			dec, err := base64.StdEncoding.DecodeString(e.Auth)
			if err != nil {
				continue
			}
			user, pass, _ = strings.Cut(string(dec), ":")
		}
		if user == "" || pass == "" {
			continue
		}
		return authConfig{Username: user, Password: pass}, true
	}
	return authConfig{}, false
}

// install copies this executable to dir/binaryName (0755) — the init container's only job.
func install(dir string) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	src, err := os.Open(self)
	if err != nil {
		return err
	}
	defer src.Close()
	dst := filepath.Join(dir, binaryName)
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, src); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	fmt.Println("installed", dst)
	return nil
}
