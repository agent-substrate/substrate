// Copyright 2026 Google LLC

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

package storagebackend

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/objectstorage"
	"github.com/agent-substrate/substrate/internal/ateerrors"
)

type fakeAzureBlobClient struct {
	downloadContainer string
	downloadObject    string
	downloadBody      io.ReadCloser
	downloadErr       error
	uploadContainer   string
	uploadObject      string
	uploadContent     []byte
	uploadErr         error
}

func (f *fakeAzureBlobClient) DownloadStream(_ context.Context, container, object string, _ *azblob.DownloadStreamOptions) (azblob.DownloadStreamResponse, error) {
	f.downloadContainer = container
	f.downloadObject = object
	return azblob.DownloadStreamResponse{
		DownloadResponse: blob.DownloadResponse{Body: f.downloadBody},
	}, f.downloadErr
}

func (f *fakeAzureBlobClient) UploadStream(_ context.Context, container, object string, reader io.Reader, _ *azblob.UploadStreamOptions) (azblob.UploadStreamResponse, error) {
	f.uploadContainer = container
	f.uploadObject = object
	content, err := io.ReadAll(reader)
	if err != nil {
		return azblob.UploadStreamResponse{}, err
	}
	f.uploadContent = content
	return azblob.UploadStreamResponse{}, f.uploadErr
}

func TestAzureClientGetObject(t *testing.T) {
	fake := &fakeAzureBlobClient{downloadBody: io.NopCloser(strings.NewReader("snapshot"))}
	store := &azureClient{client: fake}

	reader, err := store.GetObject(context.Background(), "snapshots", "actor/memory.zstd")
	if err != nil {
		t.Fatalf("GetObject() error = %v", err)
	}
	defer reader.Close()
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(content) != "snapshot" {
		t.Fatalf("GetObject() content = %q, want snapshot", content)
	}
	if fake.downloadContainer != "snapshots" || fake.downloadObject != "actor/memory.zstd" {
		t.Fatalf("DownloadStream() target = %q/%q", fake.downloadContainer, fake.downloadObject)
	}
}

func TestAzureClientPutObject(t *testing.T) {
	fake := &fakeAzureBlobClient{}
	store := &azureClient{client: fake}

	if err := store.PutObject(context.Background(), "snapshots", "actor/manifest.json", strings.NewReader("manifest")); err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}
	if fake.uploadContainer != "snapshots" || fake.uploadObject != "actor/manifest.json" {
		t.Fatalf("UploadStream() target = %q/%q", fake.uploadContainer, fake.uploadObject)
	}
	if string(fake.uploadContent) != "manifest" {
		t.Fatalf("UploadStream() content = %q, want manifest", fake.uploadContent)
	}
}

func TestAzureClientMapsMissingBlob(t *testing.T) {
	response := &http.Response{
		StatusCode: http.StatusNotFound,
		Status:     "404 Not Found",
		Header:     http.Header{"X-Ms-Error-Code": []string{"BlobNotFound"}},
		Body:       io.NopCloser(strings.NewReader("")),
		Request: &http.Request{
			Method: http.MethodGet,
			URL:    &url.URL{Scheme: "https", Host: "test.blob.core.windows.net", Path: "/snapshots/missing"},
		},
	}
	fake := &fakeAzureBlobClient{downloadErr: runtime.NewResponseError(response)}
	store := &azureClient{client: fake}

	_, err := store.GetObject(context.Background(), "snapshots", "missing")
	if !errors.Is(err, ateerrors.ReasonFailedGetExternalObject) {
		t.Fatalf("GetObject() error = %v, want %v", err, ateerrors.ReasonFailedGetExternalObject)
	}
}

var _ objectstorage.StreamingStore = (*azureClient)(nil)
