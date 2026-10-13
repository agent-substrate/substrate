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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"syscall"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

// Azure implements Store with one Azure Blob container.
type Azure struct {
	container *container.Client
}

// NewAzure returns a Store for each of containers in the account served at
// accountURL. The stores share one client, so one connection pool and one
// token cache. accountURL must use https, and the stores send requests, and
// with them the credential, only to that scheme and host.
func NewAzure(accountURL string, containers []string, credential azcore.TokenCredential) (map[string]Store, error) {
	return newTokenAzure(accountURL, containers, credential, defaultTransport(), policy.RetryOptions{})
}

func newTokenAzure(accountURL string, containers []string, credential azcore.TokenCredential, base http.RoundTripper, retry policy.RetryOptions) (map[string]Store, error) {
	endpoint, err := parseConfiguration(accountURL, containers)
	if err != nil {
		return nil, err
	}
	if endpoint.Scheme != "https" {
		return nil, fmt.Errorf("Azure Blob account URL %q must use https", accountURL)
	}
	client, err := service.NewClient(accountURL, credential, clientOptions(endpoint, base, retry))
	if err != nil {
		return nil, fmt.Errorf("creating Azure Blob service client: %w", err)
	}
	stores := make(map[string]Store, len(containers))
	for _, name := range containers {
		stores[name] = newAzure(client, name)
	}
	return stores, nil
}

func clientOptions(endpoint *url.URL, base http.RoundTripper, retry policy.RetryOptions) *service.ClientOptions {
	return &service.ClientOptions{ClientOptions: azcore.ClientOptions{
		Transport: newEndpointTransport(endpoint, base),
		Retry:     retry,
	}}
}

func newAzure(client *service.Client, containerName string) *Azure {
	return &Azure{container: client.NewContainerClient(containerName)}
}

func parseConfiguration(accountURL string, containers []string) (*url.URL, error) {
	parsed, err := url.Parse(accountURL)
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Path != "" && parsed.Path != "/" && strings.HasSuffix(parsed.Path, "/")) {
		return nil, fmt.Errorf("Azure Blob account URL %q is invalid", accountURL)
	}
	if len(containers) == 0 {
		return nil, errors.New("Azure Blob container is required")
	}
	for _, name := range containers {
		if err := ValidateContainerName(name); err != nil {
			return nil, err
		}
	}
	return parsed, nil
}

var containerNamePattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// ValidateContainerName reports whether name follows Azure's container naming
// rules: 3 to 63 lowercase letters, digits and hyphens, starting and ending
// with a letter or digit, with no two hyphens in a row. Azure rejects any
// other name on every request.
func ValidateContainerName(name string) error {
	if len(name) < 3 || len(name) > 63 || !containerNamePattern.MatchString(name) {
		return fmt.Errorf("Azure Blob container name %q is invalid: use 3-63 lowercase letters, digits and single hyphens, starting and ending with a letter or digit", name)
	}
	return nil
}

func (a *Azure) blob(name string) *blockblob.Client {
	return a.container.NewBlockBlobClient(name)
}

// StageBlock stages one deterministic block and asks Azure to verify its MD5
// in transit.
func (a *Azure) StageBlock(ctx context.Context, name, blockID string, data []byte, md5 []byte) error {
	_, err := a.blob(name).StageBlock(ctx, blockID, readSeekCloser{Reader: bytes.NewReader(data)}, &blockblob.StageBlockOptions{
		TransactionalValidation: blob.TransferValidationTypeMD5(md5),
	})
	return mapError(err)
}

// CommitBlockList atomically replaces a blob with the staged block list.
func (a *Azure) CommitBlockList(ctx context.Context, name string, blockIDs []string, metadata map[string]*string) error {
	_, err := a.blob(name).CommitBlockList(ctx, blockIDs, &blockblob.CommitBlockListOptions{Metadata: metadata})
	return mapError(err)
}

// Download opens a blob and returns the integrity metadata stored with it.
// Reading the body resumes the download after a dropped connection; an error
// that outlasts those retries is mapped like any other Azure error.
func (a *Azure) Download(ctx context.Context, name string) (Download, error) {
	resp, err := a.blob(name).DownloadStream(ctx, nil)
	if err != nil {
		return Download{}, mapError(err)
	}
	size := int64(-1)
	if resp.ContentLength != nil {
		size = *resp.ContentLength
	}
	return Download{
		Body:     mappedBody{resp.NewRetryReader(ctx, &blob.RetryReaderOptions{MaxRetries: 3})},
		Size:     size,
		Metadata: resp.Metadata,
	}, nil
}

// List returns every blob name under prefix.
func (a *Azure) List(ctx context.Context, prefix string) ([]string, error) {
	pager := a.container.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &prefix})
	var names []string
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, mapError(err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name != nil {
				names = append(names, *item.Name)
			}
		}
	}
	return names, nil
}

// Delete removes one blob. An absent blob is already deleted.
func (a *Azure) Delete(ctx context.Context, name string) error {
	_, err := a.blob(name).Delete(ctx, nil)
	err = mapError(err)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

// mappedBody maps errors from a download body the same way as request errors.
type mappedBody struct {
	io.ReadCloser
}

func (b mappedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == nil || err == io.EOF {
		return n, err
	}
	return n, mapError(err)
}

type readSeekCloser struct {
	*bytes.Reader
}

func (readSeekCloser) Close() error { return nil }

func mapError(err error) error {
	if err == nil {
		return nil
	}
	var authErr *azidentity.AuthenticationFailedError
	if errors.As(err, &authErr) {
		return mapAuthenticationError(err, authErr)
	}
	var responseErr *azcore.ResponseError
	if !errors.As(err, &responseErr) {
		if transient(err) {
			return fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
		return err
	}
	switch responseErr.ErrorCode {
	case "BlobNotFound":
		return fmt.Errorf("%w: %s", ErrNotFound, responseErr.ErrorCode)
	case "ContainerNotFound":
		return fmt.Errorf("%w: configured Azure Blob container does not exist", ErrContainerNotFound)
	case "ConditionNotMet":
		// A resumed download found the blob replaced since it was opened.
		// Retrying the operation reads the new blob from the start.
		return fmt.Errorf("%w: blob changed while it was read", ErrUnavailable)
	}
	if responseErr.StatusCode == http.StatusUnauthorized || responseErr.StatusCode == http.StatusForbidden {
		return fmt.Errorf("%w: Azure returned HTTP %d %s", ErrPermission, responseErr.StatusCode, responseErr.ErrorCode)
	}
	if responseErr.StatusCode == http.StatusRequestTimeout ||
		responseErr.StatusCode == http.StatusTooManyRequests ||
		responseErr.StatusCode >= http.StatusInternalServerError {
		return fmt.Errorf("%w: Azure returned HTTP %d", ErrUnavailable, responseErr.StatusCode)
	}
	return err
}

// mapAuthenticationError maps a failure to get a Microsoft Entra token. The
// credential reuses a valid token when a refresh fails, so this only surfaces
// when no valid token is left: on the first request after startup, or after
// the token expires during an Entra outage. Entra throttling and server
// errors are worth retrying, and so is a failure that got no response at all:
// azidentity reports a refused connection or a DNS failure with a nil
// RawResponse and keeps only the cause's message. Any other response, such as
// a rejected client or federated token, is a configuration error.
func mapAuthenticationError(err error, authErr *azidentity.AuthenticationFailedError) error {
	if authErr.RawResponse == nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	code := authErr.RawResponse.StatusCode
	if code == http.StatusRequestTimeout || code == http.StatusTooManyRequests || code >= http.StatusInternalServerError {
		return fmt.Errorf("%w: Microsoft Entra returned HTTP %d: %w", ErrUnavailable, code, err)
	}
	return err
}

// transient reports whether err is a network failure that a later attempt may
// not hit: the SDK has already retried the request, or the download resumed,
// as many times as it allows. A request the endpoint check refused and an
// expired or canceled context are not transient, and neither is a host name
// that does not exist.
func transient(err error) bool {
	var foreign *foreignEndpointError
	if errors.As(err, &foreign) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return !dnsErr.IsNotFound
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE)
}

var _ io.ReadSeekCloser = readSeekCloser{}
var _ Store = (*Azure)(nil)
