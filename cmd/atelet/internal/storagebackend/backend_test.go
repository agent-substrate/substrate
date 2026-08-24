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
	"io"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/objectstorage"
)

type customStore struct{}

func (customStore) GetObject(context.Context, string, string) (io.ReadCloser, error) {
	return nil, nil
}

func (customStore) PutObject(context.Context, string, string, io.Reader) error {
	return nil
}

func TestNewRejectsUnknownBackend(t *testing.T) {
	_, err := New(context.Background(), "custom", Options{})
	if err == nil || !strings.Contains(err.Error(), "unsupported object storage backend") {
		t.Fatalf("New() error = %v, want unsupported backend error", err)
	}
}

func TestNewUsesRegisteredBackend(t *testing.T) {
	const backend = "custom-test"
	want := customStore{}
	if err := Register(backend, func(context.Context, Options) (objectstorage.Store, error) {
		return want, nil
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	got, err := New(context.Background(), backend, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if got != want {
		t.Fatalf("New() = %T, want customStore", got)
	}
}

func TestNewAzureRequiresConfiguration(t *testing.T) {
	_, err := New(context.Background(), BackendAzure, Options{})
	if err == nil || !strings.Contains(err.Error(), "AZURE_STORAGE_ACCOUNT_URL or AZURE_STORAGE_CONNECTION_STRING") {
		t.Fatalf("New() error = %v, want Azure configuration error", err)
	}
}

func TestNewAzureFromConnectionString(t *testing.T) {
	const connectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=ZmFrZWtleQ==;EndpointSuffix=core.windows.net"
	store, err := New(context.Background(), BackendAzure, Options{AzureConnectionString: connectionString})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, ok := store.(*azureClient); !ok {
		t.Fatalf("New() = %T, want *azureClient", store)
	}
}
