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

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Synthetic credentials only. "aHViOnB3" = base64("hub:pw"); "bm9wYXNz" = base64("nopass") (no password).
const cfgJSON = `{"auths":{
  "ghcr.io":{"username":"robot","password":"s3cret"},
  "https://index.docker.io/v1/":{"auth":"aHViOnB3"},
  "registry.example.com:5000":{"auth":"bm9wYXNz"}
}}`

func writeConfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfgJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func call(t *testing.T, dir, image string) response {
	t.Helper()
	req := `{"apiVersion":"credentialprovider.kubelet.k8s.io/v1","kind":"CredentialProviderRequest","image":"` + image + `"}`
	var out bytes.Buffer
	if err := run(strings.NewReader(req), &out, dir); err != nil {
		t.Fatal(err)
	}
	var resp response
	if err := json.Unmarshal(out.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestMatchesRegistry(t *testing.T) {
	resp := call(t, writeConfig(t), "ghcr.io/planeodev/claude-agents/samples/adk-agents@sha256:"+strings.Repeat("a", 64))
	if resp.APIVersion != apiVersion || resp.Kind != "CredentialProviderResponse" || resp.CacheKeyType != "Registry" || resp.CacheDuration != "5m" {
		t.Fatalf("bad envelope: %+v", resp)
	}
	if got := resp.Auth["ghcr.io"]; got.Username != "robot" || got.Password != "s3cret" {
		t.Fatalf("ghcr.io auth = %+v", got)
	}
}

func TestDockerHubAndAuthField(t *testing.T) {
	resp := call(t, writeConfig(t), "library/busybox:1.36")
	if got := resp.Auth["docker.io"]; got.Username != "hub" || got.Password != "pw" {
		t.Fatalf("docker.io auth (from the base64 auth field) = %+v", got)
	}
}

func TestUnknownRegistryIsEmptyNotError(t *testing.T) {
	if resp := call(t, writeConfig(t), "quay.io/org/img:1"); len(resp.Auth) != 0 {
		t.Fatalf("want no auth (anonymous fallback), got %+v", resp.Auth)
	}
}

func TestIncompleteEntrySkipped(t *testing.T) {
	if resp := call(t, writeConfig(t), "registry.example.com:5000/a/b:1"); len(resp.Auth) != 0 {
		t.Fatalf("want no auth for an entry without a password, got %+v", resp.Auth)
	}
}

func TestErrors(t *testing.T) {
	var out bytes.Buffer
	if err := run(strings.NewReader(`{"apiVersion":"v0","kind":"x","image":"a"}`), &out, writeConfig(t)); err == nil {
		t.Fatal("want an error for a wrong apiVersion")
	}
	req := `{"apiVersion":"credentialprovider.kubelet.k8s.io/v1","kind":"CredentialProviderRequest","image":"ghcr.io/a/b"}`
	if err := run(strings.NewReader(req), &out, ""); err == nil {
		t.Fatal("want an error when DOCKER_CONFIG is unset")
	}
	if err := run(strings.NewReader(req), &out, t.TempDir()); err == nil {
		t.Fatal("want an error when config.json is missing")
	}
}

func TestRegistryHost(t *testing.T) {
	for in, want := range map[string]string{
		"ghcr.io/a/b@sha256:x": "ghcr.io", "localhost/a": "localhost", "localhost:5000/a": "localhost:5000",
		"busybox": "docker.io", "library/busybox": "docker.io", "my.reg:443/x/y:1": "my.reg:443",
	} {
		if got := registryHost(in); got != want {
			t.Errorf("registryHost(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestInstall(t *testing.T) {
	dir := t.TempDir()
	if err := install(dir); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(filepath.Join(dir, binaryName))
	if err != nil || st.Mode().Perm()&0o100 == 0 {
		t.Fatalf("installed binary missing or not executable: %v %v", st, err)
	}
}
