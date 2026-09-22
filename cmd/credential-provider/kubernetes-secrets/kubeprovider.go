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

// This file implements the CredentialProvider plugin API backed by Kubernetes
// Secrets. It resolves ate-secret:// URIs of the provider "k8s.io" to a Secret
// value read straight from the Kubernetes API — so Substrate never stores the
// secret, it only brokers a read the provider is authorized to perform.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// ProviderName is the ate-secret:// URI host this backend serves.
const ProviderName = "k8s.io"

// uriScheme is the only scheme a credential URI may carry.
const uriScheme = "ate-secret"

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

// Server implements credproviderpb.CredentialProviderServer over the Kubernetes
// API.
type Server struct {
	credproviderpb.UnimplementedCredentialProviderServer

	client kubernetes.Interface
	// nsAuth restricts which namespaces an atespace may resolve secrets from.
	// Nil disables authorization (dev only): every URI namespace is allowed.
	nsAuth *NamespaceAuthorizer
}

// NewServer builds a Kubernetes-backed credential provider. nsAuth enforces the
// atespace→namespace policy; pass nil to disable authorization (dev only).
func NewServer(client kubernetes.Interface, nsAuth *NamespaceAuthorizer) *Server {
	return &Server{client: client, nsAuth: nsAuth}
}

// Grants exposes the enforced atespace→namespace policy for /statusz. Nil
// when authorization is disabled.
func (s *Server) Grants() map[string][]string { return s.nsAuth.Grants() }

// FetchSecret resolves one ate-secret:// URI to its Secret value.
func (s *Server) FetchSecret(ctx context.Context, req *credproviderpb.FetchSecretRequest) (*credproviderpb.FetchSecretResponse, error) {
	ref, err := ParseURI(req.GetUri())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := s.authorize(ctx, req.GetActorSpiffeId(), ref.Namespace); err != nil {
		return nil, err
	}

	slog.InfoContext(ctx, "resolving credential",
		slog.String("provider", ProviderName),
		slog.String("namespace", ref.Namespace),
		slog.String("secret", ref.Name),
		slog.String("actor", req.GetActorSpiffeId()),
	)

	secret, err := s.client.CoreV1().Secrets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "secret %s/%s not found", ref.Namespace, ref.Name)
		}
		if k8serrors.IsForbidden(err) {
			return nil, status.Errorf(codes.PermissionDenied, "not permitted to read secret %s/%s", ref.Namespace, ref.Name)
		}
		return nil, status.Errorf(codes.Unavailable, "reading secret %s/%s: %v", ref.Namespace, ref.Name, err)
	}

	value, err := selectKey(secret.Data, ref.Key)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "secret %s/%s: %v", ref.Namespace, ref.Name, err)
	}
	return &credproviderpb.FetchSecretResponse{OpaqueBytes: value}, nil
}

// authorize enforces the atespace→namespace policy. It derives the atespace from
// the attested actor SPIFFE ID and denies unless the URI's namespace is in that
// atespace's allowed list.
func (s *Server) authorize(ctx context.Context, actorSpiffeID, namespace string) error {
	if s.nsAuth == nil {
		return nil
	}
	actor, err := resources.ActorRefFromActorSPIFFEID(actorSpiffeID)
	if err != nil {
		slog.WarnContext(ctx, "credential request denied: unusable actor identity", slog.Any("err", err))
		return status.Errorf(codes.PermissionDenied, "actor identity is required and must be a valid actor SPIFFE URI: %v", err)
	}
	if !s.nsAuth.Allowed(actor.Atespace, namespace) {
		slog.WarnContext(ctx, "credential request denied: atespace not permitted for namespace",
			slog.String("atespace", actor.Atespace), slog.String("namespace", namespace))
		return status.Errorf(codes.PermissionDenied, "atespace %q is not permitted to resolve secrets in namespace %q", actor.Atespace, namespace)
	}
	return nil
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
