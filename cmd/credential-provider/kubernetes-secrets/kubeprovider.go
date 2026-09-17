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
// Secrets. It resolves ate-secret:// URIs of the provider "kubernetes.io" to a
// Secret value read straight from the Kubernetes API — so Substrate never
// stores the secret, it only brokers a read the provider is authorized to
// perform.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// ProviderName is the ate-secret:// URI host this backend serves.
const ProviderName = "kubernetes.io"

// uriScheme is the only scheme a credential URI may carry.
const uriScheme = "ate-secret"

// SecretRef is a parsed ate-secret:// URI for the kubernetes.io provider.
//
//	ate-secret://kubernetes.io/<namespace>/<secret>[/<key>]
type SecretRef struct {
	Namespace string
	Name      string
	// Key is the data key within the Secret, or "" when the URI omits it (only
	// allowed when the secret contains one entry).
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

	if u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") {
		return SecretRef{}, fmt.Errorf("credential URI must not contain user info, a query, or a fragment")
	}

	segments := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	// <namespace>/<secret> is the minimum; an optional 3rd segment is the data
	// key.
	if len(segments) < 2 || len(segments) > 3 {
		return SecretRef{}, fmt.Errorf("credential URI %q: want <namespace>/<secret>[/<key>], got %d path segments", raw, len(segments))
	}
	for i, s := range segments {
		if s == "" {
			return SecretRef{}, fmt.Errorf("credential URI %q: empty path segment %d", raw, i)
		}
	}

	ref := SecretRef{
		Namespace: segments[0],
		Name:      segments[1],
	}
	if len(segments) == 3 {
		ref.Key = segments[2]
	}
	if len(validation.IsDNS1123Label(ref.Namespace)) != 0 || len(validation.IsDNS1123Subdomain(ref.Name)) != 0 || (ref.Key != "" && len(validation.IsConfigMapKey(ref.Key)) != 0) {
		return SecretRef{}, fmt.Errorf("credential URI contains an invalid namespace, secret name, or key")
	}
	return ref, nil
}

// Server implements credproviderpb.CredentialProviderServer over the Kubernetes
// API.
type Server struct {
	credproviderpb.UnimplementedCredentialProviderServer

	client kubernetes.Interface
	// nsAuth restricts which namespaces an atespace may resolve secrets from.
	nsAuth *NamespaceAuthorizer
}

// NewServer builds a Kubernetes credential provider with a default-deny policy.
func NewServer(client kubernetes.Interface, nsAuth *NamespaceAuthorizer) *Server {
	return &Server{client: client, nsAuth: nsAuth}
}

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
		return nil, status.Error(codes.Unavailable, "could not read secret from Kubernetes")
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
	actor, err := resources.ActorRefFromSPIFFEID(actorSpiffeID)
	if err != nil {
		slog.WarnContext(ctx, "credential request denied: unusable actor identity", slog.Any("err", err))
		return status.Error(codes.PermissionDenied, "actor identity is required and must be a valid actor SPIFFE URI")
	}
	if !s.nsAuth.Allowed(actor.Atespace, namespace) {
		slog.WarnContext(ctx, "credential request denied: atespace not permitted for namespace",
			slog.String("atespace", actor.Atespace), slog.String("namespace", namespace))
		return status.Errorf(codes.PermissionDenied, "atespace %q is not permitted to resolve secrets in namespace %q", actor.Atespace, namespace)
	}
	return nil
}

// selectKey resolves which Secret data entry to return: the URI's explicit key,
// else the sole key of a single-key Secret. A URI without a key resolving a
// multi-key Secret is an error.
func selectKey(data map[string][]byte, uriKey string) ([]byte, error) {
	if uriKey == "" {
		if len(data) != 1 {
			return nil, fmt.Errorf("no key given and the secret has %d keys; specify one in the URI", len(data))
		}
		for _, v := range data {
			return v, nil
		}
	}
	v, ok := data[uriKey]
	if !ok {
		return nil, fmt.Errorf("key %q not present", uriKey)
	}
	return v, nil
}
