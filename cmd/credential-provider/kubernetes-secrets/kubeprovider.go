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

// This file implements the CredentialProvider plugin API backed by Kubernetes
// Secrets. It resolves ate-secret:// URIs of the provider "k8s.io" to a Secret
// value read from the Kubernetes API. Substrate never stores the secret at
// rest, and the provider reuses each Secret it reads for --secret-cache-ttl.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/client-go/kubernetes"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/api/credprovider/v1"
	"go.ate.dev/substrate/internal/resources"
)

// ProviderName is the ate-secret:// URI host this backend serves.
const ProviderName = "k8s.io"

// uriScheme is the only scheme a credential URI may carry.
const uriScheme = "ate-secret"

// secretFetchTimeout caps one Secret read. The read runs detached from the
// caller's context, so it needs a deadline of its own. It matches
// injectionTimeout in atenet-egress, which caps the whole fetch.
const secretFetchTimeout = 4 * time.Second

// LocalLocator is the reserved leading path segment naming the local Kubernetes
// API server the provider runs in — the only cluster served today.
const LocalLocator = "default"

// SecretRef is a parsed ate-secret:// URI for the k8s.io provider.
//
// Only Secrets in the local cluster are addressable today:
//
//	ate-secret://k8s.io/default/<namespace>/<secret>/<key>
//
// Future work: the secret uri can grow a "cluster/<cluster>" locator for fetching remote Secrets.
//
//	ate-secret://k8s.io/cluster/<cluster>/<namespace>/<secret>/<key>
type SecretRef struct {
	Namespace string
	Name      string
	// Key is the data key within the Secret to return.
	Key string
}

// ParseURI parses a ate-secret:// URI of the kubernetes.io provider. It
// rejects any other scheme or provider name.
func ParseURI(raw string) (SecretRef, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return SecretRef{}, fmt.Errorf("parsing credential URI %q: %w", raw, err)
	}
	if u.Scheme != uriScheme {
		return SecretRef{}, fmt.Errorf("malformed credential URI %q: scheme is %q, want %q", raw, u.Scheme, uriScheme)
	}
	if u.Host != ProviderName {
		return SecretRef{}, fmt.Errorf("credential URI %q: provider is %q, this provider serves %q", raw, u.Host, ProviderName)
	}
	// The grammar is scheme/host/path only; a query or fragment means the caller
	// assumed a syntax this provider does not honor, so reject it rather than
	// silently ignore it.
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return SecretRef{}, fmt.Errorf("credential URI %q: query and fragment components are not allowed", raw)
	}
	// Reject percent-encoding in the path of secret uri.
	if u.EscapedPath() != u.Path {
		return SecretRef{}, fmt.Errorf("credential URI %q: path must not contain percent-encoding", raw)
	}

	segments := strings.Split(strings.Trim(u.Path, "/"), "/")
	for i, s := range segments {
		if s == "" {
			return SecretRef{}, fmt.Errorf("credential URI %q: empty path segment %d", raw, i)
		}
	}

	// The path must begin with the "default" locator; only local Secrets are
	// served. See SecretRef for the planned "cluster/<cluster>" remote form.
	if segments[0] != LocalLocator {
		return SecretRef{}, fmt.Errorf("credential URI %q: path must begin with %q (only local Secrets are supported), got %q", raw, LocalLocator, segments[0])
	}

	// tail is <namespace>/<secret>/<key>.
	tail := segments[1:]
	if len(tail) != 3 {
		return SecretRef{}, fmt.Errorf("credential URI %q: want %s/<namespace>/<secret>/<key>, got %d trailing segments", raw, LocalLocator, len(tail))
	}
	return SecretRef{
		Namespace: tail[0],
		Name:      tail[1],
		Key:       tail[2],
	}, nil
}

// Server implements credprovider.CredentialProviderServer over the Kubernetes
// API.
type Server struct {
	credprovider.UnimplementedCredentialProviderServer

	client kubernetes.Interface
	// nsAuth restricts which namespaces an atespace may resolve secrets from.
	// Nil disables authorization; this only happens in tests, since main
	// always requires --namespace-policy-file and loads one.
	nsAuth *NamespaceAuthorizer

	// ttl is how long a Secret read is reused. 0 disables caching.
	ttl time.Duration
	// secrets maps "namespace/name" to a cachedSecret until ttl passes.
	secrets *cache.Expiring
	// flight collapses concurrent reads of one Secret into a single Get.
	flight singleflight.Group
}

// cachedSecret is the part of a Secret that FetchSecret uses.
type cachedSecret struct {
	labels map[string]string
	data   map[string][]byte
}

// NewServer builds a Kubernetes-backed credential provider. nsAuth enforces the
// atespace→namespace policy; pass nil to disable authorization (tests only).
// Each Secret read is reused for ttl; 0 reads on every call.
func NewServer(client kubernetes.Interface, nsAuth *NamespaceAuthorizer, ttl time.Duration) *Server {
	return &Server{client: client, nsAuth: nsAuth, ttl: ttl, secrets: cache.NewExpiring()}
}

// Grants exposes the enforced atespace→namespace policy for /statusz. Nil
// when authorization is disabled.
func (s *Server) Grants() map[string][]string { return s.nsAuth.Grants() }

// FetchSecret resolves one ate-secret:// URI to its Secret value.
func (s *Server) FetchSecret(ctx context.Context, req *credprovider.FetchSecretRequest) (*credprovider.FetchSecretResponse, error) {
	ref, err := ParseURI(req.GetUri())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	atespace, err := s.authorize(ctx, req.GetActorSpiffeId(), ref.Namespace)
	if err != nil {
		return nil, err
	}

	slog.InfoContext(ctx, "resolving credential",
		slog.String("provider", ProviderName),
		slog.String("namespace", ref.Namespace),
		slog.String("secret", ref.Name),
		slog.String("actor", req.GetActorSpiffeId()),
	)

	secret, err := s.getSecret(ctx, ref.Namespace, ref.Name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			if s.nsAuth == nil {
				return nil, status.Errorf(codes.NotFound, "secret %s/%s not found", ref.Namespace, ref.Name)
			}
			// Authorization is enforced, so a missing Secret must look the
			// same as one that fails the label check below: otherwise a
			// caller could walk a list of names and learn what exists here.
			return nil, status.Errorf(codes.PermissionDenied, "atespace %q is not permitted to resolve secret %s/%s", atespace, ref.Namespace, ref.Name)
		}
		if k8serrors.IsForbidden(err) {
			return nil, status.Errorf(codes.PermissionDenied, "not permitted to read secret %s/%s", ref.Namespace, ref.Name)
		}
		return nil, status.Errorf(codes.Unavailable, "reading secret %s/%s: %v", ref.Namespace, ref.Name, err)
	}

	// Step 3: the Secret is in hand, so a grant narrowed by label can finally
	// be settled. This refusal and the NotFound above give the same code and
	// message, so a caller learns nothing from the difference.
	if s.nsAuth != nil && !s.nsAuth.AllowedSecret(atespace, ref.Namespace, secret.labels) {
		slog.WarnContext(ctx, "credential request denied: secret does not match the grant",
			slog.String("atespace", atespace),
			slog.String("namespace", ref.Namespace),
			slog.String("secret", ref.Name),
		)
		return nil, status.Errorf(codes.PermissionDenied, "atespace %q is not permitted to resolve secret %s/%s", atespace, ref.Namespace, ref.Name)
	}

	value, err := selectKey(secret.data, ref.Key)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "secret %s/%s: %v", ref.Namespace, ref.Name, err)
	}
	return &credprovider.FetchSecretResponse{OpaqueBytes: value}, nil
}

// getSecret returns the Secret namespace/name, reading it from Kubernetes when
// there is no live cache entry.
func (s *Server) getSecret(ctx context.Context, namespace, name string) (cachedSecret, error) {
	key := namespace + "/" + name
	if secret, ok := s.cached(key); ok {
		return secret, nil
	}

	// The read outlives the caller: the leader of a flight going away must
	// not fail the callers that joined it.
	ch := s.flight.DoChan(key, func() (any, error) {
		// A flight that finished between the miss above and DoChan has
		// already stored a fresh entry.
		if secret, ok := s.cached(key); ok {
			return secret, nil
		}
		getCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), secretFetchTimeout)
		defer cancel()
		secret, err := s.client.CoreV1().Secrets(namespace).Get(getCtx, name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		entry := cachedSecret{labels: secret.GetLabels(), data: secret.Data}
		if s.ttl > 0 {
			s.secrets.Set(key, entry, s.ttl)
		}
		return entry, nil
	})
	select {
	case <-ctx.Done():
		return cachedSecret{}, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return cachedSecret{}, res.Err
		}
		return res.Val.(cachedSecret), nil
	}
}

func (s *Server) cached(key string) (cachedSecret, bool) {
	v, ok := s.secrets.Get(key)
	secret, _ := v.(cachedSecret)
	return secret, ok
}

// authorize covers the first two of three authorization steps, before
// anything is read from Kubernetes:
//  1. is the caller a real actor at all (its SPIFFE ID parses)
//  2. is its atespace granted this namespace
//
// The third step, checking the fetched Secret itself against a label-narrowed
// grant, is NamespaceAuthorizer.AllowedSecret, called once the Secret is read.
func (s *Server) authorize(ctx context.Context, actorSpiffeID, namespace string) (string, error) {
	if s.nsAuth == nil {
		return "", nil
	}
	actor, err := resources.ActorRefFromActorSPIFFEID(actorSpiffeID)
	if err != nil {
		slog.WarnContext(ctx, "credential request denied: unusable actor identity", slog.Any("err", err))
		return "", status.Errorf(codes.PermissionDenied, "actor identity is required and must be a valid actor SPIFFE URI: %v", err)
	}
	if !s.nsAuth.Allowed(actor.Atespace, namespace) {
		slog.WarnContext(ctx, "credential request denied: atespace not permitted for namespace",
			slog.String("atespace", actor.Atespace), slog.String("namespace", namespace))
		return "", status.Errorf(codes.PermissionDenied, "atespace %q is not permitted to resolve secrets in namespace %q", actor.Atespace, namespace)
	}
	return actor.Atespace, nil
}

// selectKey returns the named data entry, or an error when the Secret has no
// such key.
func selectKey(data map[string][]byte, key string) ([]byte, error) {
	v, ok := data[key]
	if !ok {
		return nil, fmt.Errorf("key %q not present", key)
	}
	return v, nil
}
