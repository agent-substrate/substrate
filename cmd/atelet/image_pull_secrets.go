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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// imagePullSecretResolver reads only the pull Secrets an ActorTemplate names.
// It is scoped to one Run request and caches each Secret for all containers in
// that request. The sandbox pause image intentionally does not use it because
// it belongs to SandboxConfig rather than the ActorTemplate.
type imagePullSecretResolver struct {
	kubeClient kubernetes.Interface
	namespace  string
	refs       []*ateletpb.ImagePullSecretReference

	mu      sync.Mutex
	secrets map[string]*corev1.Secret
}

func newImagePullSecretResolver(kubeClient kubernetes.Interface, namespace string, refs []*ateletpb.ImagePullSecretReference) *imagePullSecretResolver {
	return &imagePullSecretResolver{
		kubeClient: kubeClient,
		namespace:  namespace,
		refs:       refs,
		secrets:    make(map[string]*corev1.Secret),
	}
}

func (r *imagePullSecretResolver) authenticators(ctx context.Context, image string) ([]authn.Authenticator, error) {
	var registryAuths []registryAuth
	for _, ref := range r.refs {
		secret, err := r.secret(ctx, ref.GetName())
		if err != nil {
			return nil, err
		}
		auths, err := registryAuthsFromSecret(secret)
		if err != nil {
			return nil, fmt.Errorf("invalid imagePullSecret %s/%s: %w", r.namespace, ref.GetName(), err)
		}
		registryAuths = append(registryAuths, auths...)
	}
	matches, err := registryAuthsForImage(image, registryAuths)
	if err != nil {
		return nil, fmt.Errorf("while matching image %q against imagePullSecrets: %w", image, err)
	}
	result := make([]authn.Authenticator, 0, len(matches))
	for _, match := range matches {
		result = append(result, authn.FromConfig(match.config))
	}
	return result, nil
}

func (r *imagePullSecretResolver) secret(ctx context.Context, name string) (*corev1.Secret, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if secret, ok := r.secrets[name]; ok {
		return secret, nil
	}
	secret, err := r.kubeClient.CoreV1().Secrets(r.namespace).Get(ctx, name, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		return nil, fmt.Errorf("imagePullSecrets references missing Secret %s/%s", r.namespace, name)
	}
	if err != nil {
		return nil, fmt.Errorf("while reading imagePullSecret %s/%s: %w", r.namespace, name, err)
	}
	r.secrets[name] = secret
	return secret, nil
}

type dockerConfigJSON struct {
	Auths map[string]authn.AuthConfig `json:"auths"`
}

type registryAuthScope struct {
	host       string
	repository string
}

type registryAuth struct {
	scope  registryAuthScope
	config authn.AuthConfig
}

func registryAuthsFromSecret(secret *corev1.Secret) ([]registryAuth, error) {
	var data []byte
	var auths map[string]authn.AuthConfig
	switch secret.Type {
	case corev1.SecretTypeDockerConfigJson:
		data = secret.Data[corev1.DockerConfigJsonKey]
		var config dockerConfigJSON
		if len(data) == 0 {
			return nil, fmt.Errorf("missing required key %q", corev1.DockerConfigJsonKey)
		}
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("cannot parse %q: %w", corev1.DockerConfigJsonKey, err)
		}
		auths = config.Auths
	case corev1.SecretTypeDockercfg:
		data = secret.Data[corev1.DockerConfigKey]
		if len(data) == 0 {
			return nil, fmt.Errorf("missing required key %q", corev1.DockerConfigKey)
		}
		if err := json.Unmarshal(data, &auths); err != nil {
			return nil, fmt.Errorf("cannot parse %q: %w", corev1.DockerConfigKey, err)
		}
	default:
		return nil, fmt.Errorf("unsupported Secret type %q; expected %q or %q", secret.Type, corev1.SecretTypeDockerConfigJson, corev1.SecretTypeDockercfg)
	}
	result := make([]registryAuth, 0, len(auths))
	for rawScope, config := range auths {
		host, repository, err := parseRegistryScope(rawScope)
		if err != nil {
			return nil, err
		}
		result = append(result, registryAuth{scope: registryAuthScope{host: host, repository: repository}, config: config})
	}
	return result, nil
}

func registryAuthsForImage(image string, auths []registryAuth) ([]registryAuth, error) {
	ref, err := name.ParseReference(image)
	if err != nil {
		return nil, fmt.Errorf("invalid image reference: %w", err)
	}
	host := ref.Context().Registry.RegistryStr()
	if host == "index.docker.io" {
		host = "docker.io"
	}
	repository := strings.TrimPrefix(ref.Context().Name(), ref.Context().Registry.Name()+"/")
	sort.SliceStable(auths, func(i, j int) bool {
		return len(auths[i].scope.host)+len(auths[i].scope.repository) > len(auths[j].scope.host)+len(auths[j].scope.repository)
	})
	var matches []registryAuth
	for _, auth := range auths {
		scope := auth.scope
		if registryHostsMatch(scope.host, host) &&
			(scope.repository == "" || repository == scope.repository || strings.HasPrefix(repository, scope.repository+"/")) {
			matches = append(matches, auth)
		}
	}
	return matches, nil
}

func registryHostsMatch(pattern, target string) bool {
	if pattern == target {
		return true
	}
	patternHost, patternPort := splitRegistryHost(pattern)
	targetHost, targetPort := splitRegistryHost(target)
	if patternPort != targetPort {
		return false
	}
	patternParts := strings.Split(patternHost, ".")
	targetParts := strings.Split(targetHost, ".")
	if len(patternParts) != len(targetParts) {
		return false
	}
	for i := range patternParts {
		if patternParts[i] != "*" && patternParts[i] != targetParts[i] {
			return false
		}
	}
	return true
}

func splitRegistryHost(hostPort string) (string, string) {
	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		return hostPort, ""
	}
	return host, port
}

// parseRegistryScope splits a Docker config auth key into its registry host
// and optional repository scope. For example, Docker Hub's legacy
// "https://index.docker.io/v1/" key becomes ("docker.io", ""), while
// "registry.example.com/team" becomes ("registry.example.com", "team").
func parseRegistryScope(raw string) (string, string, error) {
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil {
			return "", "", err
		}
		raw = u.Host + "/" + strings.TrimPrefix(u.Path, "/")
	}
	parts := strings.SplitN(raw, "/", 2)
	// Match kubelet's Docker Hub handling for the legacy index.docker.io/v1
	// credential key: https://github.com/kubernetes/kubernetes/blob/master/pkg/credentialprovider/keyring.go
	if parts[0] == "index.docker.io" {
		parts[0] = "docker.io"
	}
	if parts[0] == "docker.io" && len(parts) > 1 && (parts[1] == "v1" || parts[1] == "v1/") {
		return parts[0], "", nil
	}
	if strings.HasPrefix(parts[0], "*.") {
		return parts[0], strings.Trim(strings.Join(parts[1:], "/"), "/"), nil
	}
	if _, err := name.NewRegistry(parts[0], name.Insecure); err != nil {
		return "", "", err
	}
	if len(parts) == 1 {
		return parts[0], "", nil
	}
	return parts[0], strings.Trim(parts[1], "/"), nil
}
