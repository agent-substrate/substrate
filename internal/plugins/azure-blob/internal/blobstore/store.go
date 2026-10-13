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

// Package blobstore defines the Azure Blob operations used by the plugin.
package blobstore

import (
	"context"
	"errors"
	"io"
)

var (
	// ErrNotFound reports an absent blob.
	ErrNotFound = errors.New("blob not found")
	// ErrContainerNotFound reports a configured container that does not exist.
	ErrContainerNotFound = errors.New("blob container not found")
	// ErrPermission reports rejected Azure credentials or authorization.
	ErrPermission = errors.New("blob access denied")
	// ErrUnavailable reports a retryable Azure service failure.
	ErrUnavailable = errors.New("blob service unavailable")
)

// Download is a blob body, its size and its metadata.
type Download struct {
	Body io.ReadCloser
	// Size is the blob's length in bytes as the service reported it, or
	// negative if it reported none.
	Size     int64
	Metadata map[string]*string
}

// Store is the block blob surface used by the snapshot provider.
type Store interface {
	// StageBlock must consume data before returning.
	StageBlock(ctx context.Context, name, blockID string, data []byte, md5 []byte) error
	CommitBlockList(ctx context.Context, name string, blockIDs []string, metadata map[string]*string) error
	Download(ctx context.Context, name string) (Download, error)
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, name string) error
}
