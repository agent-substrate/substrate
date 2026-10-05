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

// Package provider implements substrate's CredentialProvider API over Google
// Cloud Secret Manager, resolving ate-secret:// URIs to a secret version's
// payload or to one key's value in a JSON payload. Substrate never stores the
// secret.
package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log/slog"
	"net/url"
	"regexp"
	"strings"
	"time"

	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// providerName is the ate-secret:// URI host this backend serves.
const providerName = "secretmanager.googleapis.com"

// uriScheme is the only scheme a credential URI may carry.
const uriScheme = "ate-secret"

// latestVersion is the alias for a secret's newest version. Secret Manager
// refuses it with FailedPrecondition while that version is disabled.
const latestVersion = "latest"

// Name formats, checked so a malformed name fails as InvalidArgument rather
// than as whatever Secret Manager makes of it.
var (
	projectID     = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`)
	projectNumber = regexp.MustCompile(`^[0-9]+$`)
	secretID      = regexp.MustCompile(`^[A-Za-z0-9_-]{1,255}$`)
)

// region matches a Google Cloud region such as us-central1. The location
// becomes part of the regional endpoint's hostname, so it must not admit a dot
// or anything else that could change the host.
var region = regexp.MustCompile(`^[a-z]+-[a-z]+[0-9]+$`)

// crc32cTable is the polynomial Secret Manager checksums payloads with.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// secretRef is a parsed ate-secret:// URI: a secret version's resource name,
// optionally followed by a key into its JSON payload.
//
//	ate-secret://secretmanager.googleapis.com/projects/<project>/secrets/<secret>/versions/<version>
//	ate-secret://secretmanager.googleapis.com/projects/<project>/secrets/<secret>/versions/<version>/keys/<key>
//
// A regional secret adds locations/<location> after the project, as its
// resource name does.
type secretRef struct {
	// Project is the Google Cloud project ID or number owning the secret.
	Project string
	// Location is the region of a regional secret; empty for a global one.
	Location string
	// Secret is the Secret Manager secret ID.
	Secret string
	// Version is a positive integer or "latest".
	Version string
	// Key, if set, is the top-level JSON key whose value is the credential.
	Key string
}

// resourceName is the Secret Manager resource name of the referenced version.
func (r secretRef) resourceName() string {
	if r.Location != "" {
		return fmt.Sprintf("projects/%s/locations/%s/secrets/%s/versions/%s", r.Project, r.Location, r.Secret, r.Version)
	}
	return fmt.Sprintf("projects/%s/secrets/%s/versions/%s", r.Project, r.Secret, r.Version)
}

// parseURI parses a secretmanager.googleapis.com ate-secret:// URI, rejecting
// any other scheme or provider and any malformed resource name.
func parseURI(raw string) (secretRef, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return secretRef{}, fmt.Errorf("parsing credential URI %q: %w", raw, err)
	}
	if u.Scheme != uriScheme {
		return secretRef{}, fmt.Errorf("malformed credential URI %q: scheme is %q, want %q", raw, u.Scheme, uriScheme)
	}
	if u.Host != providerName {
		return secretRef{}, fmt.Errorf("credential URI %q: provider is %q, this provider serves %q", raw, u.Host, providerName)
	}
	// Reject, rather than ignore, components the grammar does not define.
	if u.User != nil {
		return secretRef{}, fmt.Errorf("credential URI %q: user information is not allowed", raw)
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return secretRef{}, fmt.Errorf("credential URI %q: query and fragment components are not allowed", raw)
	}
	// A Secret Manager resource name never needs percent-encoding.
	if u.EscapedPath() != u.Path {
		return secretRef{}, fmt.Errorf("credential URI %q: path must not contain percent-encoding", raw)
	}

	const want = "projects/<project>[/locations/<location>]/secrets/<secret>/versions/<version>[/keys/<key>]"
	// Split without trimming, so an extra slash becomes an empty segment.
	if !strings.HasPrefix(u.Path, "/") {
		return secretRef{}, fmt.Errorf("credential URI %q: want %s, got path %q", raw, want, u.Path)
	}
	segments := strings.Split(u.Path[1:], "/")
	for i, seg := range segments {
		if seg == "" {
			return secretRef{}, fmt.Errorf("credential URI %q: empty path segment %d", raw, i)
		}
	}
	if len(segments) < 2 || segments[0] != "projects" {
		return secretRef{}, fmt.Errorf("credential URI %q: want %s, got path %q", raw, want, u.Path)
	}
	if !projectID.MatchString(segments[1]) && !projectNumber.MatchString(segments[1]) {
		return secretRef{}, fmt.Errorf("credential URI %q: project %q must be a project ID or number", raw, segments[1])
	}
	ref := secretRef{Project: segments[1]}
	rest := segments[2:]

	if len(rest) >= 2 && rest[0] == "locations" {
		if !region.MatchString(rest[1]) {
			return secretRef{}, fmt.Errorf("credential URI %q: location %q must be a region, such as us-central1", raw, rest[1])
		}
		ref.Location = rest[1]
		rest = rest[2:]
	}

	if (len(rest) != 4 && len(rest) != 6) || rest[0] != "secrets" || rest[2] != "versions" {
		return secretRef{}, fmt.Errorf("credential URI %q: want %s, got path %q", raw, want, u.Path)
	}
	if !secretID.MatchString(rest[1]) {
		return secretRef{}, fmt.Errorf("credential URI %q: secret ID %q must be 1-255 letters, digits, underscores or dashes", raw, rest[1])
	}
	if !validVersion(rest[3]) {
		return secretRef{}, fmt.Errorf("credential URI %q: version %q must be a positive integer or %q", raw, rest[3], latestVersion)
	}
	ref.Secret, ref.Version = rest[1], rest[3]
	if len(rest) == 6 {
		if rest[4] != "keys" {
			return secretRef{}, fmt.Errorf("credential URI %q: want %s, got path %q", raw, want, u.Path)
		}
		// Dot segments are path traversal, never a key.
		if rest[5] == "." || rest[5] == ".." {
			return secretRef{}, fmt.Errorf("credential URI %q: key %q is not allowed", raw, rest[5])
		}
		ref.Key = rest[5]
	}
	return ref, nil
}

// validVersion reports whether v names a Secret Manager version: the "latest"
// alias or a positive integer without leading zeros.
func validVersion(v string) bool {
	if v == latestVersion {
		return true
	}
	if v == "" || v[0] == '0' {
		return false
	}
	for _, c := range v {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// SecretAccessor is the subset of *secretmanager.Client the server uses.
type SecretAccessor interface {
	AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error)
}

// Accessors returns the Secret Manager client for a location: "" for global,
// or a region.
type Accessors func(location string) (SecretAccessor, error)

// Server implements credproviderpb.CredentialProviderServer over Secret
// Manager. mTLS admits only the egress gateway, which names each request's
// actor; a secret resolves only in a project the policy grants that actor's
// atespace.
type Server struct {
	credproviderpb.UnimplementedCredentialProviderServer

	clients      Accessors
	authz        *ProjectAuthorizer
	fetchTimeout time.Duration
}

// NewServer returns a Server that reads through clients the secrets in projects
// authz grants (none if authz is nil). Each Secret Manager call gets at most
// fetchTimeout, which must be positive.
func NewServer(clients Accessors, authz *ProjectAuthorizer, fetchTimeout time.Duration) *Server {
	return &Server{clients: clients, authz: authz, fetchTimeout: fetchTimeout}
}

// FetchSecret resolves an ate-secret:// URI to its secret version's payload, or
// to the string value of the URI's key in it, returned verbatim.
func (s *Server) FetchSecret(ctx context.Context, req *credproviderpb.FetchSecretRequest) (*credproviderpb.FetchSecretResponse, error) {
	ref, err := parseURI(req.GetUri())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.authorize(ctx, req.GetActorSpiffeId(), ref.Project); err != nil {
		return nil, err
	}

	slog.InfoContext(ctx, "resolving credential",
		slog.String("provider", providerName),
		slog.String("project", ref.Project),
		slog.String("location", ref.Location),
		slog.String("secret", ref.Secret),
		slog.String("version", ref.Version),
		slog.String("key", ref.Key),
		slog.String("actor", req.GetActorSpiffeId()),
	)

	// The gateway sets no deadline and the client retries for up to a minute;
	// answer DeadlineExceeded before Envoy's ext_proc timeout abandons the call.
	fetchCtx, cancel := context.WithTimeout(ctx, s.fetchTimeout)
	defer cancel()
	client, err := s.clients(ref.Location)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "secret version %s: no Secret Manager client: %v", ref.resourceName(), err)
	}
	resp, err := client.AccessSecretVersion(fetchCtx, &secretmanagerpb.AccessSecretVersionRequest{Name: ref.resourceName()})
	if err != nil {
		return nil, accessError(ref, err)
	}

	payload := resp.GetPayload()
	if payload == nil {
		return nil, status.Errorf(codes.NotFound, "secret version %s has no payload", ref.resourceName())
	}
	// Fail closed on a CRC32C mismatch rather than inject a corrupted credential.
	if payload.DataCrc32C != nil {
		if got := int64(crc32.Checksum(payload.GetData(), crc32cTable)); got != payload.GetDataCrc32C() {
			return nil, status.Errorf(codes.Unavailable, "secret version %s: payload CRC32C mismatch", ref.resourceName())
		}
	}
	if ref.Key == "" {
		return &credproviderpb.FetchSecretResponse{OpaqueBytes: payload.GetData()}, nil
	}
	value, err := keyValue(ref, payload.GetData())
	if err != nil {
		return nil, err
	}
	return &credproviderpb.FetchSecretResponse{OpaqueBytes: value}, nil
}

// authorize denies unless the policy grants project to the atespace of the
// actor SPIFFE ID the gateway asserts.
func (s *Server) authorize(ctx context.Context, actorSpiffeID, project string) error {
	atespace, err := actorAtespace(actorSpiffeID)
	if err != nil {
		slog.WarnContext(ctx, "credential request denied: unusable actor identity", slog.Any("err", err))
		return status.Errorf(codes.PermissionDenied, "actor identity is required and must be a valid actor SPIFFE URI: %v", err)
	}
	if !s.authz.Allowed(atespace, project) {
		slog.WarnContext(ctx, "credential request denied: atespace not permitted for project",
			slog.String("atespace", atespace), slog.String("project", project))
		return status.Errorf(codes.PermissionDenied, "atespace %q is not permitted to resolve secrets in project %q", atespace, project)
	}
	return nil
}

// accessError maps a Secret Manager error to a code for the egress gateway,
// which answers Unavailable and DeadlineExceeded with a retryable 503 and
// anything else with a 403. Failures a retry cannot fix keep a non-retryable
// code; unrecognized ones are treated as transient.
func accessError(ref secretRef, err error) error {
	name := ref.resourceName()
	msg := status.Convert(err).Message()
	switch code := status.Code(err); code {
	case codes.NotFound:
		return status.Errorf(codes.NotFound, "secret version %s not found", name)
	case codes.PermissionDenied, codes.Unauthenticated:
		return status.Errorf(codes.PermissionDenied, "not permitted to access secret version %s: %s", name, msg)
	case codes.FailedPrecondition, codes.InvalidArgument, codes.DeadlineExceeded:
		return status.Errorf(code, "accessing secret version %s: %s", name, msg)
	default:
		return status.Errorf(codes.Unavailable, "accessing secret version %s: %s", name, msg)
	}
}

// keyValue returns the string value of ref.Key in the JSON object data. Only
// that value is decoded, so an unrepresentable sibling such as 1e999 does not
// fail it. Errors omit the decoder's message, which can quote the payload.
func keyValue(ref secretRef, data []byte) ([]byte, error) {
	var obj map[string]json.RawMessage
	// A top-level null decodes into a nil map without error.
	if err := json.Unmarshal(data, &obj); err != nil || obj == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "secret version %s: payload is not a JSON object", ref.resourceName())
	}
	raw, ok := obj[ref.Key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "secret version %s: payload has no key %q", ref.resourceName(), ref.Key)
	}
	// Into any rather than string: a string target takes null as a no-op.
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "secret version %s: value of key %q is not a JSON string", ref.resourceName(), ref.Key)
	}
	s, ok := v.(string)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "secret version %s: value of key %q is not a JSON string", ref.resourceName(), ref.Key)
	}
	return []byte(s), nil
}
