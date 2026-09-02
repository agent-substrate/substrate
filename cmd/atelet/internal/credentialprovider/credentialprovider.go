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

// Package credentialprovider resolves registry credentials by invoking kubelet
// image credential provider plugins
// (https://kubernetes.io/docs/tasks/administer-cluster/kubelet-credential-provider/).
//
// Reusing the kubelet's contract keeps atelet free of any cloud SDK: the same
// binary authenticates to Artifact Registry on GKE and to ECR on EKS, using
// whichever plugin its node ships. The plugin and its config reach atelet as
// read-only host mounts; see manifests/ate-install/atelet.yaml.
package credentialprovider

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	credentialproviderv1 "k8s.io/kubelet/pkg/apis/credentialprovider/v1"
)

// Keychain is a go-containerregistry authn.Keychain backed by kubelet image
// credential provider plugins. It is safe for concurrent use.
type Keychain struct {
	plugins []*plugin
}

var (
	_ authn.Keychain        = (*Keychain)(nil)
	_ authn.ContextKeychain = (*Keychain)(nil)
)

// New loads the CredentialProviderConfig at configPath and resolves each
// provider's executable inside binDir. Both mirror the kubelet flags of the
// same name; pointing atelet at the node's values makes it authenticate like
// that node's kubelet.
func New(configPath, binDir string) (*Keychain, error) {
	plugins, err := loadConfig(configPath, binDir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(plugins))
	for _, p := range plugins {
		names = append(names, p.name)
	}
	slog.Info("Loaded image credential providers",
		slog.String("config", configPath),
		slog.String("binDir", binDir),
		slog.Any("providers", names),
	)
	return &Keychain{plugins: plugins}, nil
}

// Resolve implements authn.Keychain.
func (k *Keychain) Resolve(target authn.Resource) (authn.Authenticator, error) {
	return k.ResolveContext(context.Background(), target)
}

// ResolveContext implements authn.ContextKeychain, returning the credentials of
// the first provider that both claims the image and has an auth entry for it.
// When none does it returns authn.Anonymous, which is what public registries
// want.
func (k *Keychain) ResolveContext(ctx context.Context, target authn.Resource) (authn.Authenticator, error) {
	// Registry and repository with no tag or digest
	// ("us-docker.pkg.dev/proj/repo/image"), the granularity the protocol's
	// cache keys are defined at.
	image := target.String()

	for _, p := range k.plugins {
		claims, err := p.claims(image)
		if err != nil {
			return nil, err
		}
		if !claims {
			continue
		}
		auth, cached, err := p.provide(ctx, image)
		if err != nil {
			return nil, err
		}
		if auth == nil {
			slog.Debug("Image credential provider returned no credentials",
				slog.String("provider", p.name), slog.String("image", image))
			continue
		}
		// Info: the record of who authenticated a pull is the first thing a 401
		// investigation needs, and it costs one line per cache miss (images are
		// digest-pinned, so resolution is per image per node, not per actor).
		// cached distinguishes a served entry from a subprocess exec.
		slog.InfoContext(ctx, "Resolved image credentials from credential provider",
			slog.String("provider", p.name), slog.String("image", image), slog.Bool("cached", cached))
		return authn.FromConfig(authn.AuthConfig{
			Username: auth.Username,
			Password: auth.Password,
		}), nil
	}
	return authn.Anonymous, nil
}

// cacheEntry is one plugin response held until expiry. A nil auth is cached
// too, so "no credentials for you" costs no further subprocess.
type cacheEntry struct {
	auth      *credentialproviderv1.AuthConfig
	expiresAt time.Time
}

// plugin is one configured provider executable and its response cache.
type plugin struct {
	name                 string
	path                 string
	args                 []string
	env                  []string
	matchImages          []string
	defaultCacheDuration time.Duration

	mu    sync.Mutex
	cache map[string]cacheEntry
	// now is time.Now, overridden in tests.
	now func() time.Time
}

// claims reports whether this plugin is configured to handle image.
func (p *plugin) claims(image string) (bool, error) {
	for _, m := range p.matchImages {
		matched, err := matchesImage(m, image)
		if err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

// globalCacheKey is the cache key for plugins that answer identically for
// every image they claim (cacheKeyType: Global).
const globalCacheKey = "global"

// minCacheDuration is the lifetime given to a response that asks not to be
// cached at all. Providers still default to zero for compatibility with the
// removed in-tree credential providers -- auth-provider-gcp does, and GKE sets
// nothing to override it -- which would fork a subprocess for every cold pull.
// A minute is well inside any registry token's lifetime, and is the
// defaultCacheDuration GKE's own node config already declares.
//
// Zero is also how a plugin in service-account pass-through mode says its
// credentials must not be shared between workloads, but that mode requires
// tokenAttributes, which newPlugin refuses -- so no such provider reaches this
// cache. Supporting tokenAttributes means revisiting this.
//
// Only zero is raised. A provider naming a shorter lifetime has opted into
// caching and may be describing a genuinely short-lived credential, so
// stretching it could serve an expired one.
const minCacheDuration = time.Minute

// provide returns cached credentials for image, or execs the plugin and caches
// what it returns; cached reports which. A nil AuthConfig with a nil error
// means the plugin has no credentials for this image.
func (p *plugin) provide(ctx context.Context, image string) (auth *credentialproviderv1.AuthConfig, cached bool, err error) {
	if entry, ok := p.lookup(image); ok {
		return entry, true, nil
	}

	resp, err := p.exec(ctx, image)
	if err != nil {
		return nil, false, err
	}

	key, err := bestAuthKey(resp.Auth, image)
	if err != nil {
		return nil, false, err
	}
	if key != "" {
		matched := resp.Auth[key]
		auth = &matched
	}

	p.store(image, resp, auth)
	return auth, false, nil
}

// lookup tries every key type the plugin might have stored under, most
// specific first: a plugin's cacheKeyType is not known until it answers once.
func (p *plugin) lookup(image string) (*credentialproviderv1.AuthConfig, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, key := range []string{image, registryOf(image), globalCacheKey} {
		entry, ok := p.cache[key]
		if !ok {
			continue
		}
		if !p.timeNow().Before(entry.expiresAt) {
			delete(p.cache, key)
			continue
		}
		return entry.auth, true
	}
	return nil, false
}

// store caches auth under the key type the plugin asked for. A response
// carrying an unrecognized cacheKeyType, or an explicit zero duration, is not
// cached at all.
func (p *plugin) store(image string, resp *credentialproviderv1.CredentialProviderResponse, auth *credentialproviderv1.AuthConfig) {
	var key string
	switch resp.CacheKeyType {
	case credentialproviderv1.ImagePluginCacheKeyType:
		key = image
	case credentialproviderv1.RegistryPluginCacheKeyType:
		key = registryOf(image)
	case credentialproviderv1.GlobalPluginCacheKeyType:
		key = globalCacheKey
	default:
		slog.Warn("Image credential provider returned an unknown cacheKeyType; not caching",
			slog.String("provider", p.name), slog.String("cacheKeyType", string(resp.CacheKeyType)))
		return
	}

	duration := p.defaultCacheDuration
	if resp.CacheDuration != nil {
		duration = resp.CacheDuration.Duration
	}
	if duration <= 0 {
		duration = minCacheDuration
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.cache[key] = cacheEntry{auth: auth, expiresAt: p.timeNow().Add(duration)}
}

func (p *plugin) timeNow() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}
