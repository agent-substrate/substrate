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

//go:build integration

package blobstore

import (
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

// NewAzureWithSharedKey returns a Store authenticated with a storage account
// key, for the Azurite integration tests. It compiles only with the
// integration build tag, so the plugin binary has no account key path.
func NewAzureWithSharedKey(accountURL, containerName, accountName, accountKey string) (*Azure, error) {
	endpoint, err := parseConfiguration(accountURL, []string{containerName})
	if err != nil {
		return nil, err
	}
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("creating Azure shared key credential: %w", err)
	}
	client, err := service.NewClientWithSharedKeyCredential(accountURL, credential, clientOptions(endpoint, defaultTransport(), policy.RetryOptions{}, defaultStallTimeout))
	if err != nil {
		return nil, fmt.Errorf("creating Azure Blob service client: %w", err)
	}
	return newAzure(client, containerName), nil
}
