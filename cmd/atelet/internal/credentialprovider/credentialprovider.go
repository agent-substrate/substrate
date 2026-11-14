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
	"math/rand/v2"
	"strings"
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

// ResolveContext implements authn.ContextKeychain. It returns the first
// candidate, or authn.Anonymous if there are none.
func (k *Keychain) ResolveContext(ctx context.Context, target authn.Resource) (authn.Authenticator, error) {
	candidates, err := k.Candidates(ctx, target)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return authn.Anonymous, nil
	}
	return candidates[0], nil
}

// Candidates returns the credentials of all providers matching target, in the
// order to try them: most specific auth key first, then config order. Failing
// providers are logged and skipped, like the kubelet.
func (k *Keychain) Candidates(ctx context.Context, target authn.Resource) ([]authn.Authenticator, error) {
	// Registry and repository with no tag or digest
	// ("us-docker.pkg.dev/proj/repo/image"), the granularity the protocol's
	// cache keys are defined at.
	image := target.String()

	byKey := make(map[string][]credentialproviderv1.AuthConfig)
	for _, p := range k.plugins {
		claims, err := p.claims(image)
		if err != nil {
			return nil, err
		}
		if !claims {
			continue
		}
		authMap, cached, err := p.provide(ctx, image)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if err != nil {
			slog.WarnContext(ctx, "Image credential provider failed; skipping it",
				slog.String("provider", p.name), slog.String("image", image), slog.Any("err", err))
			continue
		}
		keys, err := matchingAuthKeys(authMap, image)
		if err != nil {
			slog.WarnContext(ctx, "Image credential provider returned an invalid auth key; skipping it",
				slog.String("provider", p.name), slog.String("image", image), slog.Any("err", err))
			continue
		}
		if len(keys) == 0 {
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
		for _, key := range keys {
			byKey[key] = append(byKey[key], authMap[key])
		}
	}

	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sortAuthKeys(keys)
	var candidates []authn.Authenticator
	seen := make(map[credentialproviderv1.AuthConfig]struct{})
	for _, key := range keys {
		for _, auth := range byKey[key] {
			if _, dup := seen[auth]; dup {
				continue
			}
			seen[auth] = struct{}{}
			candidates = append(candidates, authn.FromConfig(authn.AuthConfig{
				Username: auth.Username,
				Password: auth.Password,
			}))
		}
	}
	return candidates, nil
}

// cacheEntry is one plugin response's auth map, held until expiry. The whole
// map is kept because a Registry or Global entry serves other images, which
// may match a different key. Responses with no usable credentials are not
// cached so a transient STS or metadata-server throttle cannot poison pulls.
type cacheEntry struct {
	auth      map[string]credentialproviderv1.AuthConfig
	expiresAt time.Time
}

type pluginFlight struct {
	waiters     int
	finished    bool
	cancel      context.CancelFunc
	done        chan struct{}
	leaderImage string
	auth        map[string]credentialproviderv1.AuthConfig
	keyType     credentialproviderv1.PluginCacheKeyType
	err         error
}

func (p *plugin) releaseWaiter(flightKey string, f *pluginFlight) {
	var cancel context.CancelFunc
	p.mu.Lock()
	f.waiters--
	if f.waiters == 0 && !f.finished {
		if p.flights[flightKey] == f {
			delete(p.flights, flightKey)
		}
		cancel = f.cancel
	}
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// plugin is one configured provider executable and its response cache.
type plugin struct {
	name                 string
	path                 string
	args                 []string
	env                  []string
	matchImages          []string
	defaultCacheDuration time.Duration

	mu      sync.Mutex
	cache   map[string]cacheEntry
	flights map[string]*pluginFlight
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
const (
	minCacheDuration  = time.Minute
	maxExecAttempts   = 5
	retryBaseInterval = 200 * time.Millisecond
	retryMaxInterval  = 2 * time.Second
)

// usableAuthMap returns a copy of raw containing only entries with a non-empty
// password. Providers throttled by an upstream token service can exit 0 with an
// empty map or an empty password; treating those as unusable keeps Candidates
// from trying a blank secret and keeps store from caching the failure.
func usableAuthMap(raw map[string]credentialproviderv1.AuthConfig) map[string]credentialproviderv1.AuthConfig {
	if len(raw) == 0 {
		return nil
	}
	var out map[string]credentialproviderv1.AuthConfig
	for k, v := range raw {
		if v.Password == "" {
			continue
		}
		if out == nil {
			out = make(map[string]credentialproviderv1.AuthConfig, len(raw))
		}
		out[k] = v
	}
	return out
}

func isTransientThrottleOutput(msg string) bool {
	if msg == "" {
		return false
	}
	lower := strings.ToLower(msg)
	for _, phrase := range []string{
		"too many requests",
		"quota_exceeded",
		"quota exceeded",
		"resource_exhausted",
		"rate limit",
		"temporarily unavailable",
		"service unavailable",
		"bad gateway",
		"gateway timeout",
		"internal server error",
		"i/o timeout",
		"tls handshake timeout",
		"connection reset",
	} {
		if strings.Contains(lower, phrase) {
			return true
		}
	}
	for _, code := range []string{"429", "500", "502", "503", "504"} {
		for _, prefix := range []string{
			"status " + code,
			"status: " + code,
			"status=" + code,
			"status code " + code,
			"status code: " + code,
			"http " + code,
			"http/1.1 " + code,
			"http/2 " + code,
			"code " + code,
			"code=" + code,
			"error " + code,
		} {
			if strings.Contains(lower, prefix) {
				return true
			}
		}
	}
	return false
}

// provide returns the plugin's auth map for image, from the cache if
// possible; cached reports which. Concurrent cold misses for the same registry
// share one plugin execution so a burst of pulls on a fresh node does not
// stampede the node's metadata or STS endpoint.
func (p *plugin) provide(ctx context.Context, image string) (auth map[string]credentialproviderv1.AuthConfig, cached bool, err error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if auth, ok := p.lookup(image); ok {
		return auth, true, nil
	}

	regKey := "reg:" + registryOf(image)
	auth, keyType, leaderImage, isLeader, err := p.joinOrRunFlight(ctx, regKey, image)
	if err != nil {
		return nil, false, err
	}
	if isLeader || image == leaderImage ||
		keyType == credentialproviderv1.RegistryPluginCacheKeyType ||
		keyType == credentialproviderv1.GlobalPluginCacheKeyType {
		return auth, !isLeader && len(auth) > 0, nil
	}

	// The plugin scopes credentials per repository (Image), and the registry
	// flight ran for a different repository. Check the cache first, then run a
	// repository-scoped flight.
	if auth, ok := p.lookup(image); ok {
		return auth, true, nil
	}
	auth, _, _, isImgLeader, err := p.joinOrRunFlight(ctx, "img:"+image, image)
	if err != nil {
		return nil, false, err
	}
	return auth, !isImgLeader && len(auth) > 0, nil
}

func (p *plugin) joinOrRunFlight(ctx context.Context, flightKey, image string) (
	auth map[string]credentialproviderv1.AuthConfig,
	keyType credentialproviderv1.PluginCacheKeyType,
	leaderImage string,
	isLeader bool,
	err error,
) {
	p.mu.Lock()
	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		return nil, "", "", false, err
	}
	if cachedAuth, ok := p.lookupLocked(image, time.Now()); ok {
		p.mu.Unlock()
		return cachedAuth, credentialproviderv1.RegistryPluginCacheKeyType, image, false, nil
	}
	f, ok := p.flights[flightKey]
	if ok {
		f.waiters++
		p.mu.Unlock()
	} else {
		execCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		f = &pluginFlight{
			waiters:     1,
			cancel:      cancel,
			done:        make(chan struct{}),
			leaderImage: image,
		}
		p.flights[flightKey] = f
		isLeader = true
		p.mu.Unlock()
		go p.runFlight(execCtx, flightKey, f)
	}

	select {
	case <-f.done:
		return f.auth, f.keyType, f.leaderImage, isLeader, f.err
	case <-ctx.Done():
		p.releaseWaiter(flightKey, f)
		return nil, "", "", false, ctx.Err()
	}
}

func (p *plugin) runFlight(ctx context.Context, flightKey string, f *pluginFlight) {
	resp, err := p.execWithRetry(ctx, f.leaderImage)
	var auth map[string]credentialproviderv1.AuthConfig
	var keyType credentialproviderv1.PluginCacheKeyType
	if err == nil && resp != nil {
		auth = usableAuthMap(resp.Auth)
		keyType = resp.CacheKeyType
		if len(auth) > 0 {
			clean := *resp
			clean.Auth = auth
			p.store(f.leaderImage, &clean)
		}
	}

	f.auth = auth
	f.keyType = keyType
	f.err = err

	p.mu.Lock()
	if p.flights[flightKey] == f {
		delete(p.flights, flightKey)
	}
	f.finished = true
	cancel := f.cancel
	p.mu.Unlock()

	cancel()
	close(f.done)
}

func (p *plugin) execWithRetry(ctx context.Context, image string) (*credentialproviderv1.CredentialProviderResponse, error) {
	var lastResp *credentialproviderv1.CredentialProviderResponse
	var lastErr error
	for attempt := range maxExecAttempts {
		if attempt > 0 {
			base := min(retryBaseInterval<<(attempt-1), retryMaxInterval)
			wait := base + time.Duration(rand.Int64N(int64(base/2)+1))
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
		resp, stderr, err := p.exec(ctx, image)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil || attempt == maxExecAttempts-1 || !isTransientThrottleOutput(stderr) {
				return nil, err
			}
			slog.WarnContext(ctx, "Retrying transient image credential provider error",
				slog.String("provider", p.name), slog.Int("attempt", attempt+1), slog.Any("err", err))
			continue
		}
		lastResp = resp
		lastErr = nil
		if len(usableAuthMap(resp.Auth)) > 0 || attempt == maxExecAttempts-1 || !isTransientThrottleOutput(stderr) {
			return resp, nil
		}
		slog.WarnContext(ctx, "Retrying image credential provider after empty credentials with throttle stderr",
			slog.String("provider", p.name), slog.Int("attempt", attempt+1), slog.String("stderr", stderr))
	}
	return lastResp, lastErr
}

// lookup tries every key type the plugin might have stored under, most
// specific first: a plugin's cacheKeyType is not known until it answers once.
func (p *plugin) lookup(image string) (map[string]credentialproviderv1.AuthConfig, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lookupLocked(image, time.Now())
}

func (p *plugin) lookupLocked(image string, now time.Time) (map[string]credentialproviderv1.AuthConfig, bool) {
	for _, key := range []string{image, registryOf(image), globalCacheKey} {
		entry, ok := p.cache[key]
		if !ok {
			continue
		}
		if !now.Before(entry.expiresAt) {
			delete(p.cache, key)
			continue
		}
		return entry.auth, true
	}
	return nil, false
}

// store caches resp's auth map under the key type the plugin asked for. A
// response carrying no usable credentials or an unrecognized cacheKeyType is
// not cached.
func (p *plugin) store(image string, resp *credentialproviderv1.CredentialProviderResponse) {
	if len(resp.Auth) == 0 {
		return
	}
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
	now := time.Now()
	// Sweep on every store: lookup only evicts the keys it is asked for, so
	// an Image-keyed plugin would otherwise keep every repository it has seen.
	for k, entry := range p.cache {
		if !now.Before(entry.expiresAt) {
			delete(p.cache, k)
		}
	}
	p.cache[key] = cacheEntry{auth: resp.Auth, expiresAt: now.Add(duration)}
}
