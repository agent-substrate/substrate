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

// Package kubeprovider implements the CredentialProvider plugin API backed by
// Kubernetes Secrets. It resolves substrate-secret:// URIs of the class
// "kubernetes.io" to a Secret value read straight from the Kubernetes API — so
// Substrate never stores the secret, it only brokers a read the provider is
// authorized to perform.
package kubeprovider

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

	"github.com/agent-substrate/substrate/internal/actorspiffe"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// ProviderName is the substrate-secret:// URI host this backend serves.
const ProviderName = "kubernetes.io"

// uriScheme is the only scheme a credential URI may carry.
const uriScheme = "substrate-secret"

// SecretRef is a parsed substrate-secret:// URI for the kubernetes.io class.
//
//	substrate-secret://kubernetes.io/<provider name>/<namespace>/<secret>[/<key>]
type SecretRef struct {
	// ProviderName is the registered provider instance name. It is opaque to
	// this backend (a single kubernetes.io provider serves every name); it is
	// retained only so it can be logged and, later, used to scope providers.
	ProviderName string
	Namespace    string
	Name         string
	// Key is the data key within the Secret, or "" when the URI omits it (the
	// server then falls back to its configured default key).
	Key string
}

// ParseURI parses a substrate-secret:// URI of the kubernetes.io class. It
// rejects any other scheme or provider class so a misrouted URI fails loudly
// rather than reading the wrong store.
func ParseURI(raw string) (SecretRef, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return SecretRef{}, fmt.Errorf("parsing credential URI %q: %w", raw, err)
	}
	if u.Scheme != uriScheme {
		return SecretRef{}, fmt.Errorf("credential URI %q: scheme is %q, want %q", raw, u.Scheme, uriScheme)
	}
	if u.Host != ProviderName {
		return SecretRef{}, fmt.Errorf("credential URI %q: provider class is %q, this provider serves %q", raw, u.Host, ProviderName)
	}

	segments := strings.Split(strings.Trim(u.Path, "/"), "/")
	// <provider name>/<namespace>/<secret> is the minimum; an optional 4th
	// segment is the data key.
	if len(segments) < 3 || len(segments) > 4 {
		return SecretRef{}, fmt.Errorf("credential URI %q: want <provider name>/<namespace>/<secret>[/<key>], got %d path segments", raw, len(segments))
	}
	for i, s := range segments {
		if s == "" {
			return SecretRef{}, fmt.Errorf("credential URI %q: empty path segment %d", raw, i)
		}
	}

	ref := SecretRef{
		ProviderName: segments[0],
		Namespace:    segments[1],
		Name:         segments[2],
	}
	if len(segments) == 4 {
		ref.Key = segments[3]
	}
	return ref, nil
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

// RequestSecret resolves one substrate-secret:// URI to its Secret value.
func (s *Server) RequestSecret(ctx context.Context, req *credproviderpb.RequestSecretRequest) (*credproviderpb.RequestSecretResponse, error) {
	ref, err := ParseURI(req.GetUri())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := s.authorize(ctx, req.GetContext(), ref.Namespace); err != nil {
		return nil, err
	}

	slog.InfoContext(ctx, "resolving credential",
		slog.String("provider", ref.ProviderName),
		slog.String("namespace", ref.Namespace),
		slog.String("secret", ref.Name),
		slog.String("actor", req.GetContext().GetActorIdentity()),
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
	return &credproviderpb.RequestSecretResponse{Secret: value}, nil
}

// authorize enforces the atespace→namespace policy. When no authorizer is
// configured it is a no-op (dev only). Otherwise it derives the atespace from
// the attested actor identity and denies unless the URI's namespace is in that
// atespace's allowed list.
func (s *Server) authorize(ctx context.Context, reqCtx *credproviderpb.SecretRequestContext, namespace string) error {
	if s.nsAuth == nil {
		return nil
	}
	atespace, _, err := actorspiffe.Parse(reqCtx.GetActorIdentity())
	if err != nil {
		slog.WarnContext(ctx, "credential request denied: unusable actor identity", slog.Any("err", err))
		return status.Error(codes.PermissionDenied, "actor identity is required and must be a valid actor SPIFFE URI")
	}
	if !s.nsAuth.Allowed(atespace, namespace) {
		slog.WarnContext(ctx, "credential request denied: atespace not permitted for namespace",
			slog.String("atespace", atespace), slog.String("namespace", namespace))
		return status.Errorf(codes.PermissionDenied, "atespace %q is not permitted to resolve secrets in namespace %q", atespace, namespace)
	}
	return nil
}

// selectKey resolves which Secret data entry to return: the URI's explicit key,
// else the sole key of a single-key Secret. A URI without a key resolving a
// multi-key Secret is an error rather than an ambiguous guess.
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
