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
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/objectstorage"
	"github.com/agent-substrate/substrate/internal/ateerrors"
)

type azureBlobClient interface {
	DownloadStream(context.Context, string, string, *azblob.DownloadStreamOptions) (azblob.DownloadStreamResponse, error)
	UploadStream(context.Context, string, string, io.Reader, *azblob.UploadStreamOptions) (azblob.UploadStreamResponse, error)
}

type azureClient struct {
	client azureBlobClient
}

// NewAzureClient adapts an Azure Blob Storage client to an object storage Store.
func NewAzureClient(client *azblob.Client) objectstorage.Store {
	return &azureClient{client: client}
}

func (a *azureClient) GetObject(ctx context.Context, container, object string) (io.ReadCloser, error) {
	response, err := a.client.DownloadStream(ctx, container, object, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
			return nil, fmt.Errorf("%w: Azure container:%q, blob:%q", ateerrors.ReasonFailedGetExternalObject, container, object)
		}
		return nil, err
	}
	return response.Body, nil
}

func (a *azureClient) PutObject(ctx context.Context, container, object string, reader io.Reader) error {
	_, err := a.client.UploadStream(ctx, container, object, reader, nil)
	if err != nil {
		return fmt.Errorf("while putting Azure blob: %w", err)
	}
	return nil
}

func (a *azureClient) SupportsStreamingWrites() {}
