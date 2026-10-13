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

package blobstore

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

type memoryAzure struct {
	mu      sync.Mutex
	staged  map[string]map[string][]byte
	blobs   map[string][]byte
	digests map[string]string
}

func newMemoryAzure() *memoryAzure {
	return &memoryAzure{
		staged:  map[string]map[string][]byte{},
		blobs:   map[string][]byte{},
		digests: map[string]string{},
	}
}

func (m *memoryAzure) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name := strings.TrimPrefix(r.URL.Path, "/snapshots/")
	switch {
	case r.Method == http.MethodPut && r.URL.Query().Get("comp") == "block":
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		sum := md5.Sum(data)
		if got := r.Header.Get("Content-MD5"); got != base64.StdEncoding.EncodeToString(sum[:]) {
			http.Error(w, "bad MD5", http.StatusBadRequest)
			return
		}
		if m.staged[name] == nil {
			m.staged[name] = map[string][]byte{}
		}
		m.staged[name][r.URL.Query().Get("blockid")] = data
		w.WriteHeader(http.StatusCreated)
	case r.Method == http.MethodPut && r.URL.Query().Get("comp") == "blocklist":
		var list struct {
			Latest []string `xml:"Latest"`
		}
		if err := xml.NewDecoder(r.Body).Decode(&list); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var data []byte
		for _, id := range list.Latest {
			block, ok := m.staged[name][id]
			if !ok {
				writeAzureError(w, http.StatusBadRequest, "InvalidBlockList")
				return
			}
			data = append(data, block...)
		}
		m.blobs[name] = data
		m.digests[name] = r.Header.Get("x-ms-meta-substrate_sha256")
		delete(m.staged, name)
		w.WriteHeader(http.StatusCreated)
	case r.Method == http.MethodGet && r.URL.Query().Get("comp") == "list":
		prefix := r.URL.Query().Get("prefix")
		var names []string
		for blob := range m.blobs {
			if strings.HasPrefix(blob, prefix) {
				names = append(names, blob)
			}
		}
		slices.Sort(names)
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults><Blobs>`)
		for _, blob := range names {
			fmt.Fprintf(w, "<Blob><Name>%s</Name><Properties><Content-Length>%d</Content-Length></Properties></Blob>", blob, len(m.blobs[blob]))
		}
		fmt.Fprint(w, `</Blobs><NextMarker /></EnumerationResults>`)
	case r.Method == http.MethodGet:
		data, ok := m.blobs[name]
		if !ok {
			writeAzureError(w, http.StatusNotFound, "BlobNotFound")
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		w.Header().Set("x-ms-meta-substrate_sha256", m.digests[name])
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	case r.Method == http.MethodDelete:
		if _, ok := m.blobs[name]; !ok {
			writeAzureError(w, http.StatusNotFound, "BlobNotFound")
			return
		}
		delete(m.blobs, name)
		delete(m.digests, name)
		w.WriteHeader(http.StatusAccepted)
	default:
		http.Error(w, r.Method+" "+r.URL.String(), http.StatusNotImplemented)
	}
}

func writeAzureError(w http.ResponseWriter, statusCode int, code string) {
	w.Header().Set("x-ms-error-code", code)
	w.WriteHeader(statusCode)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

func TestAzureStore(t *testing.T) {
	backend := newMemoryAzure()
	server := httptest.NewServer(backend)
	defer server.Close()
	client, err := service.NewClientWithNoCredential(server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	store := newAzure(client, "snapshots")
	ctx := context.Background()
	data := []byte("snapshot")
	sum := md5.Sum(data)
	digest := "abc123"
	blockID := base64.StdEncoding.EncodeToString([]byte("block-0"))

	if err := store.StageBlock(ctx, "prefix/file", blockID, data, sum[:]); err != nil {
		t.Fatalf("StageBlock: %v", err)
	}
	if err := store.CommitBlockList(ctx, "prefix/file", []string{blockID}, map[string]*string{"substrate_sha256": &digest}); err != nil {
		t.Fatalf("CommitBlockList: %v", err)
	}
	download, err := store.Download(ctx, "prefix/file")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	got, err := io.ReadAll(download.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := download.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("download = %q, want %q", got, data)
	}
	if download.Size != int64(len(data)) {
		t.Fatalf("download size = %d, want %d", download.Size, len(data))
	}
	if value := download.Metadata["Substrate_sha256"]; value == nil || *value != digest {
		t.Fatalf("metadata = %v, want digest %q", download.Metadata, digest)
	}
	names, err := store.List(ctx, "prefix/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !slices.Equal(names, []string{"prefix/file"}) {
		t.Fatalf("List = %v", names)
	}
	if err := store.Delete(ctx, "prefix/file"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := store.Delete(ctx, "prefix/file"); err != nil {
		t.Fatalf("idempotent Delete: %v", err)
	}
	if _, err := store.Download(ctx, "prefix/file"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing Download = %v, want ErrNotFound", err)
	}
}

func TestConstructorsRejectInvalidConfiguration(t *testing.T) {
	if _, err := NewAzure("://bad", []string{"snapshots"}, nil); err == nil {
		t.Fatal("NewAzure accepted an invalid account URL")
	}
	if _, err := NewAzure("https://account.example", nil, staticCredential{}); err == nil {
		t.Fatal("NewAzure accepted no containers")
	}
	if _, err := NewAzure("https://account.example", []string{"snapshots", "Snapshots"}, staticCredential{}); err == nil || !strings.Contains(err.Error(), `"Snapshots"`) {
		t.Fatalf("NewAzure(Snapshots) = %v, want an invalid container name error", err)
	}
}

func TestMapError(t *testing.T) {
	sentinel := errors.New("plain")
	if got := mapError(nil); got != nil {
		t.Fatalf("mapError(nil) = %v", got)
	}
	if got := mapError(sentinel); !errors.Is(got, sentinel) {
		t.Fatalf("mapError(plain) = %v", got)
	}
	for _, tc := range []struct {
		name       string
		statusCode int
		errorCode  string
		want       error
	}{
		{name: "blob missing", statusCode: http.StatusNotFound, errorCode: "BlobNotFound", want: ErrNotFound},
		{name: "container missing", statusCode: http.StatusNotFound, errorCode: "ContainerNotFound", want: ErrContainerNotFound},
		{name: "authentication", statusCode: http.StatusForbidden, errorCode: "AuthenticationFailed", want: ErrPermission},
		{name: "authorization", statusCode: http.StatusForbidden, errorCode: "AuthorizationPermissionMismatch", want: ErrPermission},
		{name: "other forbidden", statusCode: http.StatusForbidden, errorCode: "AuthorizationServiceMismatch", want: ErrPermission},
		{name: "unauthorized", statusCode: http.StatusUnauthorized, errorCode: "InvalidAuthenticationInfo", want: ErrPermission},
		{name: "timeout", statusCode: http.StatusRequestTimeout, errorCode: "OperationTimedOut", want: ErrUnavailable},
		{name: "throttled", statusCode: http.StatusTooManyRequests, errorCode: "TooManyRequests", want: ErrUnavailable},
		{name: "server", statusCode: http.StatusInternalServerError, errorCode: "InternalError", want: ErrUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := &azcore.ResponseError{StatusCode: tc.statusCode, ErrorCode: tc.errorCode}
			if got := mapError(err); !errors.Is(got, tc.want) {
				t.Fatalf("mapError = %v, want %v", got, tc.want)
			}
		})
	}
	other := &azcore.ResponseError{StatusCode: http.StatusBadRequest, ErrorCode: "InvalidRequest", RawResponse: &http.Response{Request: &http.Request{URL: &url.URL{}}}}
	if got := mapError(other); got != other {
		t.Fatalf("mapError(other) = %v, want original", got)
	}
}

func TestMapErrorTransportFailures(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		transient bool
	}{
		{name: "connection refused", err: &url.Error{Op: "Get", URL: "https://a", Err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}}, transient: true},
		{name: "connection reset", err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, transient: true},
		{name: "DNS server failure", err: &net.DNSError{Err: "server misbehaving", Name: "a", IsTemporary: true}, transient: true},
		{name: "DNS timeout", err: &net.DNSError{Err: "i/o timeout", Name: "a", IsTimeout: true}, transient: true},
		{name: "truncated body", err: io.ErrUnexpectedEOF, transient: true},
		{name: "connection closed before response", err: &url.Error{Op: "Get", URL: "https://a", Err: io.EOF}, transient: true},
		{name: "TLS handshake timeout", err: timeoutError{}, transient: true},
		{name: "unknown host", err: &net.DNSError{Err: "no such host", Name: "a", IsNotFound: true}},
		{name: "foreign endpoint", err: &foreignEndpointError{scheme: "https", host: "other", want: "https://a"}},
		{name: "canceled", err: &url.Error{Op: "Get", URL: "https://a", Err: context.Canceled}},
		{name: "deadline", err: &url.Error{Op: "Get", URL: "https://a", Err: context.DeadlineExceeded}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := mapError(tc.err)
			if errors.Is(got, ErrUnavailable) != tc.transient {
				t.Fatalf("mapError(%v) = %v, want ErrUnavailable: %v", tc.err, got, tc.transient)
			}
			if !errors.Is(got, tc.err) {
				t.Fatalf("mapError(%v) = %v, lost the original error", tc.err, got)
			}
		})
	}
	changed := &azcore.ResponseError{StatusCode: http.StatusPreconditionFailed, ErrorCode: "ConditionNotMet"}
	if got := mapError(changed); !errors.Is(got, ErrUnavailable) {
		t.Fatalf("mapError(ConditionNotMet) = %v, want ErrUnavailable", got)
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "net/http: TLS handshake timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestAzureConnectionFailureIsUnavailable(t *testing.T) {
	server := httptest.NewTLSServer(http.NotFoundHandler())
	transport := server.Client().Transport
	accountURL := server.URL
	server.Close()

	store := newTokenStore(t, accountURL, staticCredential{}, transport, policy.RetryOptions{MaxRetries: -1})
	ctx := t.Context()
	if _, err := store.Download(ctx, "prefix/file"); !errors.Is(err, ErrUnavailable) {
		t.Errorf("Download = %v, want ErrUnavailable", err)
	}
	if _, err := store.List(ctx, "prefix/"); !errors.Is(err, ErrUnavailable) {
		t.Errorf("List = %v, want ErrUnavailable", err)
	}
	if err := store.StageBlock(ctx, "prefix/file", "YmxvY2s=", []byte("x"), nil); !errors.Is(err, ErrUnavailable) {
		t.Errorf("StageBlock = %v, want ErrUnavailable", err)
	}
}

func TestAzureRejectedRequestsArePermissionErrors(t *testing.T) {
	for _, tc := range []struct {
		statusCode int
		errorCode  string
	}{
		{statusCode: http.StatusForbidden, errorCode: "AuthorizationServiceMismatch"},
		{statusCode: http.StatusForbidden, errorCode: "AuthorizationResourceTypeMismatch"},
		{statusCode: http.StatusUnauthorized, errorCode: "InvalidAuthenticationInfo"},
	} {
		t.Run(tc.errorCode, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeAzureError(w, tc.statusCode, tc.errorCode)
			}))
			defer server.Close()
			store := newTokenStore(t, server.URL, staticCredential{}, server.Client().Transport, policy.RetryOptions{MaxRetries: -1})
			ctx := t.Context()
			if _, err := store.Download(ctx, "prefix/file"); !errors.Is(err, ErrPermission) {
				t.Errorf("Download = %v, want ErrPermission", err)
			}
			if _, err := store.List(ctx, "prefix/"); !errors.Is(err, ErrPermission) {
				t.Errorf("List = %v, want ErrPermission", err)
			}
			if err := store.StageBlock(ctx, "prefix/file", "YmxvY2s=", []byte("x"), nil); !errors.Is(err, ErrPermission) {
				t.Errorf("StageBlock = %v, want ErrPermission", err)
			}
		})
	}
}

func TestAzureDownloadFailureAfterResumesIsUnavailable(t *testing.T) {
	const data = "snapshot bytes"
	var mu sync.Mutex
	var gets []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, r.Method, http.StatusNotImplemented)
			return
		}
		mu.Lock()
		gets = append(gets, r.Header.Get("x-ms-range"))
		mu.Unlock()
		offset := 0
		if rng := r.Header.Get("x-ms-range"); rng != "" {
			if _, err := fmt.Sscanf(rng, "bytes=%d-", &offset); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		remaining := data[offset:]
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("x-ms-meta-substrate_sha256", "digest")
		w.Header().Set("Content-Length", fmt.Sprint(len(remaining)))
		if offset > 0 {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, len(data)-1, len(data)))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		// The first response sends one byte before dropping the connection,
		// and every resumed response drops it before sending any.
		if offset == 0 {
			_, _ = io.WriteString(w, remaining[:1])
		}
		w.(http.Flusher).Flush()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}))
	defer server.Close()

	store := newTokenStore(t, server.URL, staticCredential{}, server.Client().Transport, policy.RetryOptions{MaxRetries: -1})
	download, err := store.Download(t.Context(), "prefix/file")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer download.Body.Close()
	got, err := io.ReadAll(download.Body)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("reading the body = %v, want ErrUnavailable", err)
	}
	mu.Lock()
	defer mu.Unlock()
	// The first GET plus three resumes after the byte already read.
	want := []string{"", "bytes=1-", "bytes=1-", "bytes=1-"}
	if !slices.Equal(gets, want) || string(got) != data[:1] {
		t.Fatalf("GET ranges = %q and body %q, want ranges %q and body %q", gets, got, want, data[:1])
	}
}

func TestValidateContainerName(t *testing.T) {
	for _, name := range []string{"abc", "snapshots", "run-123", "a1-b2-c3", strings.Repeat("a", 63)} {
		if err := ValidateContainerName(name); err != nil {
			t.Errorf("ValidateContainerName(%q) = %v, want nil", name, err)
		}
	}
	for _, name := range []string{"", "ab", strings.Repeat("a", 64), "Snapshots", "snap_shots", "snap.shots", "a/b", "-abc", "abc-", "ab--cd", "$root", "REPLACE_WITH_CONTAINERS"} {
		if err := ValidateContainerName(name); err == nil {
			t.Errorf("ValidateContainerName(%q) = nil, want an error", name)
		}
	}
}

// countingCredential counts token requests.
type countingCredential struct {
	calls atomic.Int32
}

func (c *countingCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	c.calls.Add(1)
	return azcore.AccessToken{Token: testToken, ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func TestAzureContainersShareOneTokenCache(t *testing.T) {
	server := httptest.NewTLSServer(&recorder{})
	defer server.Close()
	credential := &countingCredential{}
	stores, err := newTokenAzure(server.URL, []string{"snapshots", "other"}, credential, server.Client().Transport, policy.RetryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for name, store := range stores {
		if err := store.Delete(t.Context(), "prefix/file"); err != nil {
			t.Fatalf("Delete in %s: %v", name, err)
		}
	}
	if got := credential.calls.Load(); got != 1 {
		t.Fatalf("credential asked for %d tokens across %d containers, want 1", got, len(stores))
	}
}

// TestAzureTokenFailures drives a real workload identity credential against a
// fake Microsoft Entra endpoint and checks how a failed token request is
// reported by a storage call.
func TestAzureTokenFailures(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      int
		refused     bool
		unavailable bool
	}{
		{name: "service unavailable", status: http.StatusServiceUnavailable, unavailable: true},
		{name: "server error", status: http.StatusInternalServerError, unavailable: true},
		{name: "throttled", status: http.StatusTooManyRequests, unavailable: true},
		{name: "timeout", status: http.StatusRequestTimeout, unavailable: true},
		{name: "connection refused", refused: true, unavailable: true},
		{name: "rejected client", status: http.StatusBadRequest},
		{name: "unauthorized", status: http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var tokenRequests atomic.Int32
			entra := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/tenant/v2.0/.well-known/openid-configuration" {
					base := "https://" + r.Host + "/tenant"
					fmt.Fprintf(w, `{"token_endpoint":%q,"authorization_endpoint":%q,"issuer":%q}`,
						base+"/oauth2/v2.0/token", base+"/oauth2/v2.0/authorize", base+"/v2.0")
					return
				}
				tokenRequests.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				fmt.Fprint(w, `{"error":"test_error","error_description":"token request failed"}`)
			}))
			defer entra.Close()
			entraClient, entraURL := entra.Client(), entra.URL
			if tc.refused {
				entra.Close()
			}
			tokenFile := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenFile, []byte("service-account-token"), 0o600); err != nil {
				t.Fatal(err)
			}
			credential, err := azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
				ClientOptions: azcore.ClientOptions{
					Cloud:     cloud.Configuration{ActiveDirectoryAuthorityHost: entraURL + "/"},
					Transport: entraClient,
					Retry:     policy.RetryOptions{MaxRetries: -1},
				},
				ClientID:                 "client",
				TenantID:                 "tenant",
				TokenFilePath:            tokenFile,
				DisableInstanceDiscovery: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			account := &recorder{}
			accountServer := httptest.NewTLSServer(account)
			defer accountServer.Close()
			store := newTokenStore(t, accountServer.URL, credential, accountServer.Client().Transport, policy.RetryOptions{MaxRetries: -1})

			err = store.Delete(t.Context(), "prefix/file")
			var authErr *azidentity.AuthenticationFailedError
			if !errors.As(err, &authErr) {
				t.Fatalf("Delete = %v, want an azidentity.AuthenticationFailedError", err)
			}
			if errors.Is(err, ErrUnavailable) != tc.unavailable {
				t.Fatalf("Delete = %v, want ErrUnavailable: %v", err, tc.unavailable)
			}
			if !tc.refused && tokenRequests.Load() == 0 {
				t.Fatal("the fake Entra endpoint received no token request")
			}
			if requests, _ := account.seen(); requests != 0 {
				t.Fatalf("account endpoint received %d requests without a token, want none", requests)
			}
		})
	}
}
