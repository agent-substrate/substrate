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
	"fmt"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/objectstorage"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/api/option"
)

const (
	// BackendGCS selects the Google Cloud Storage adapter.
	BackendGCS = "gcs"
	// BackendS3 selects the S3-compatible adapter.
	BackendS3 = "s3"
	// BackendAzure selects the Azure Blob Storage adapter.
	BackendAzure = "azure"
)

// Options configures provider adapters.
type Options struct {
	S3UsePathStyle        bool
	AzureAccountURL       string
	AzureConnectionString string
}

// Factory constructs an object store adapter.
type Factory func(context.Context, Options) (objectstorage.Store, error)

var (
	factoriesMu sync.RWMutex
	factories   = map[string]Factory{
		BackendGCS: func(ctx context.Context, _ Options) (objectstorage.Store, error) {
			client, err := storage.NewClient(ctx)
			if err != nil {
				return nil, fmt.Errorf("creating GCS client: %w", err)
			}
			return NewGCSClient(client), nil
		},
		BackendS3: func(ctx context.Context, opts Options) (objectstorage.Store, error) {
			cfg, err := config.LoadDefaultConfig(ctx)
			if err != nil {
				return nil, fmt.Errorf("loading S3 config: %w", err)
			}
			client := s3.NewFromConfig(cfg, func(options *s3.Options) {
				options.UsePathStyle = opts.S3UsePathStyle
			})
			return NewS3Client(client), nil
		},
		BackendAzure: func(_ context.Context, opts Options) (objectstorage.Store, error) {
			if opts.AzureConnectionString != "" {
				client, err := azblob.NewClientFromConnectionString(opts.AzureConnectionString, nil)
				if err != nil {
					return nil, fmt.Errorf("creating Azure Blob client from connection string: %w", err)
				}
				return NewAzureClient(client), nil
			}
			if opts.AzureAccountURL == "" {
				return nil, fmt.Errorf("Azure Blob Storage requires AZURE_STORAGE_ACCOUNT_URL or AZURE_STORAGE_CONNECTION_STRING")
			}
			credential, err := azidentity.NewDefaultAzureCredential(nil)
			if err != nil {
				return nil, fmt.Errorf("creating default Azure credential: %w", err)
			}
			client, err := azblob.NewClient(opts.AzureAccountURL, credential, nil)
			if err != nil {
				return nil, fmt.Errorf("creating Azure Blob client: %w", err)
			}
			return NewAzureClient(client), nil
		},
	}
)

// Register adds an object storage backend factory under name.
func Register(name string, factory Factory) error {
	if name == "" {
		return fmt.Errorf("object storage backend name must not be empty")
	}
	if factory == nil {
		return fmt.Errorf("object storage backend %q has a nil factory", name)
	}
	factoriesMu.Lock()
	defer factoriesMu.Unlock()
	if _, exists := factories[name]; exists {
		return fmt.Errorf("object storage backend %q is already registered", name)
	}
	factories[name] = factory
	return nil
}

// New constructs a registered object storage backend.
func New(ctx context.Context, backend string, opts Options) (objectstorage.Store, error) {
	if backend == "" {
		backend = BackendGCS
	}
	factoriesMu.RLock()
	factory, ok := factories[backend]
	factoriesMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unsupported object storage backend %q", backend)
	}
	return factory(ctx, opts)
}

// NewAnonymousGCS constructs the unauthenticated GCS adapter used for public assets.
func NewAnonymousGCS(ctx context.Context) (objectstorage.Store, error) {
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		return nil, fmt.Errorf("creating anonymous GCS client: %w", err)
	}
	return NewGCSClient(client), nil
}
