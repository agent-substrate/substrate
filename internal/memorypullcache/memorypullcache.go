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

package memorypullcache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"k8s.io/utils/lru"
)

type MemoryPullCache struct {
	gcpAuthenticator authn.Authenticator

	localhostRegistryReplacement string

	// Map from hexadecimal sha256 hash of image to byte contents of composed
	// tarball
	cache *lru.Cache
}

func NewMemoryPullCache(ctx context.Context, gcpAuthenticator authn.Authenticator, localhostRegistryReplacement string) (*MemoryPullCache, error) {
	c := &MemoryPullCache{
		// TODO: Need a smarter cache with bounds on total consumed size, not
		// just number of entries.  Potentially also try to share the cache
		// across ateoms on the same machine.
		//
		// It would have to be a directory with files named after the sha256
		// hash.  The benefit would be that a read might be found in the
		// filesystem cache, or perhaps the folder could be on SSD.
		//
		// From the perspective of stable operation, without hidden kernel
		// caches that could fill up or have weird behavior, it might be better
		// to just have two levels.  Store some images in ateom memory, and the
		// rest are kept in a shared GCS cache.
		cache:                        lru.New(256),
		localhostRegistryReplacement: localhostRegistryReplacement,
	}

	c.gcpAuthenticator = gcpAuthenticator

	return c, nil
}

func (c *MemoryPullCache) Fetch(ctx context.Context, ref string) (io.ReadCloser, error) {
	// when running in kind we need to rewrite the registry endpoint similar to the
	// containerd mirror config used in https://kind.sigs.k8s.io/docs/user/local-registry/
	// for now we have simple opt-in support to rewrite local registries
	rewritten := false
	if c.localhostRegistryReplacement != "" {
		newRef := c.rewriteLocalRegistry(ref)
		if newRef != ref {
			ref = newRef
			rewritten = true
		}
	}
	var nameOpts []name.Option
	// match docker behavior, permit http image pulls for local registries
	// this avoids needing to distribute TLS certs all around for local development
	if rewritten || isLocalRegistry(ref) {
		nameOpts = append(nameOpts, name.Insecure)
	}

	parsedRef, err := name.ParseReference(ref, nameOpts...)
	if err != nil {
		return nil, fmt.Errorf("while parsing reference: %w", err)
	}

	// If the image ref included a digest, check for a hit in the pull cache.
	requestedDigest, digestWasIncluded := parsedRef.(name.Digest)
	if digestWasIncluded {
		slog.InfoContext(
			ctx,
			"Ref includes digest, checking for cache hit",
			slog.String("ref", ref),
			slog.String("digest", requestedDigest.DigestStr()),
		)

		if vAny, ok := c.cache.Get(requestedDigest.DigestStr()); ok {
			slog.InfoContext(
				ctx,
				"Cache hit",
				slog.String("ref", ref),
				slog.String("digest", requestedDigest.DigestStr()),
			)
			return io.NopCloser(bytes.NewReader(vAny.([]byte))), nil
		}
	}

	slog.InfoContext(
		ctx,
		"Cache miss",
		slog.String("ref", ref),
	)

	// If we didn't have a cache hit, we are on the slow path of pulling the
	// image from the registry.  This is a chatty process, with multiple round
	// trips to the registry.

	var remoteOptions []remote.Option
	remoteOptions = append(remoteOptions, remote.WithPlatform(v1.Platform{
		Architecture: runtime.GOARCH,
		OS:           "linux",
	}))

	registry := parsedRef.Context().Registry.RegistryStr()
	if registry == "gcr.io" || strings.HasSuffix(registry, ".gcr.io") || registry == "pkg.dev" || strings.HasSuffix(registry, ".pkg.dev") {
		if c.gcpAuthenticator != nil {
			remoteOptions = append(remoteOptions, remote.WithAuth(c.gcpAuthenticator))
		}
	}

	img, err := remote.Image(parsedRef, remoteOptions...)
	if err != nil {
		return nil, fmt.Errorf("in remote.Image: %w", err)
	}

	size, err := img.Size()
	if err != nil {
		return nil, fmt.Errorf("in img.Size(): %w", err)
	}
	if size > 100*1024*1024 {
		slog.InfoContext(ctx,
			"Image is too large to cache",
			slog.String("ref", ref),
			slog.Int64("size", size),
		)
		return mutate.Extract(img), err
	}

	tarData := mutate.Extract(img)
	defer tarData.Close()

	memData, err := io.ReadAll(tarData)
	if err != nil {
		return nil, fmt.Errorf("while reading image: %w", err)
	}

	if digestWasIncluded {
		// If the user requested multi-arch image, the digest they request will
		// not be the same as the digest of the image we actually downloaded
		// from the registry.  We need to place the cache entry under the digest
		// they requested.
		c.cache.Add(requestedDigest.DigestStr(), memData)
		slog.InfoContext(
			ctx,
			"Populated image cache",
			slog.String("ref", ref),
			slog.String("digest", requestedDigest.DigestStr()),
		)
	}

	return io.NopCloser(bytes.NewReader(memData)), nil
}

func registryHost(ref string) string {
	parts := strings.SplitN(ref, "/", 2)
	reg, err := name.NewRegistry(parts[0], name.Insecure)
	if err != nil {
		return ""
	}
	hostPart := reg.Name()
	if h, _, err := net.SplitHostPort(hostPart); err == nil {
		return h
	}
	return hostPart
}

func isLocalhostOrLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return true
	}
	return false
}

func isLocalRegistry(ref string) bool {
	// by default docker permits localhost and 127.0.0.0/8
	// we also permit IPv6 loopback here
	return isLocalhostOrLoopback(registryHost(ref))
}

func (c *MemoryPullCache) rewriteLocalRegistry(ref string) string {
	if isLocalRegistry(ref) {
		parts := strings.SplitN(ref, "/", 2)
		return c.localhostRegistryReplacement + "/" + parts[1]
	}
	return ref
}
