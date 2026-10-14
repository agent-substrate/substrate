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

package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/url"
	"slices"
	"strings"

	"github.com/agent-substrate/substrate/internal/plugins/azure-blob/internal/blobstore"
	"github.com/agent-substrate/substrate/pkg/assetfetch"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AssetServer serves AssetProvider from Azure Blob Storage. It reads asset
// URIs in the account's native blob URL form,
// <account-url>/<container>/<blob>, from the containers in stores only.
//
// Like the in-tree plugin, it refuses any blob stored under a snapshot or tag
// location, in any container, before reading anything, and downloads into a
// staging directory private to the plugin, copying an asset to the caller's
// file only once its size and sha256 check out (see assetfetch).
type AssetServer struct {
	objectstorev1.UnimplementedAssetProviderServer

	// scheme, host and path are the account URL's, lower-cased where Azure
	// ignores case; path has no trailing slash.
	scheme, host, path string
	stores             map[string]blobstore.Store
	stager             *assetfetch.Stager
}

// NewAssetServer returns an AssetServer for the account at accountURL that
// reads assets from the containers in stores, which maps each container name
// to its store, writes caller files only below root, and stages downloads in
// stagingDir, which must not be below root, holding at most stagingCapacity
// bytes there at once.
func NewAssetServer(accountURL string, stores map[string]blobstore.Store, root, stagingDir string, stagingCapacity int64) (*AssetServer, error) {
	account, err := url.Parse(accountURL)
	if err != nil || account.Host == "" || account.User != nil || account.RawQuery != "" || account.Fragment != "" {
		return nil, fmt.Errorf("account URL %q is invalid", accountURL)
	}
	if len(stores) == 0 {
		return nil, errors.New("at least one asset container is required")
	}
	for container, store := range stores {
		if err := blobstore.ValidateContainerName(container); err != nil {
			return nil, err
		}
		if store == nil {
			return nil, fmt.Errorf("blob store for asset container %q is required", container)
		}
	}
	stager, err := assetfetch.NewStager(root, stagingDir, stagingCapacity)
	if err != nil {
		return nil, err
	}
	return &AssetServer{
		scheme: strings.ToLower(account.Scheme),
		host:   strings.ToLower(account.Host),
		path:   strings.TrimSuffix(account.Path, "/"),
		stores: maps.Clone(stores),
		stager: stager,
	}, nil
}

// FetchAsset downloads the blob at asset_uri into the staging directory and,
// if it is at most max_bytes long and hashes to sha256, replaces the existing
// file at write_path with it (see assetfetch.Stager.Fetch).
func (s *AssetServer) FetchAsset(ctx context.Context, req *objectstorev1.FetchAssetRequest) (*objectstorev1.FetchAssetResponse, error) {
	store, blobName, err := s.parseAssetURI(req.GetAssetUri())
	if err != nil {
		return nil, err
	}
	uri := req.GetAssetUri()
	open := func(ctx context.Context) (io.ReadCloser, int64, error) {
		download, err := store.Download(ctx, blobName)
		if err != nil {
			return nil, 0, err
		}
		return download.Body, download.Size, nil
	}
	if err := s.stager.Fetch(ctx, req, uri, open, assetStatus); err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Fetched sandbox asset; size and sha256 verified before writing",
		slog.String("url", uri), slog.String("sha256", req.GetSha256()), slog.String("writePath", req.GetWritePath()))
	return &objectstorev1.FetchAssetResponse{}, nil
}

// parseAssetURI accepts <account-url>/<container>/<blob> whose blob name is in
// canonical form and is not stored under a snapshot location, in a container
// assets may be read from, and returns that container's store and the blob
// name.
func (s *AssetServer) parseAssetURI(uri string) (blobstore.Store, string, error) {
	u, err := assetfetch.ParseURI(uri)
	if err != nil {
		return nil, "", err
	}
	account := s.scheme + "://" + s.host + s.path
	// The store sends requests only to the account's scheme and host; this
	// turns any other URL into a clear refusal before a request is built.
	if !strings.EqualFold(u.Scheme, s.scheme) || !strings.EqualFold(u.Host, s.host) || !strings.HasPrefix(u.Path, s.path+"/") {
		return nil, "", status.Errorf(codes.InvalidArgument, "asset URI %s is not a blob URL in storage account %s", uri, account)
	}
	container, blobName, ok := strings.Cut(strings.TrimPrefix(u.Path, s.path+"/"), "/")
	if !ok || container == "" || blobName == "" {
		return nil, "", status.Errorf(codes.InvalidArgument, "asset URI %s must name a container and a blob", uri)
	}
	// Before the container check, so a snapshot blob is refused as one in
	// every container, including the snapshot containers.
	if err := assetfetch.CheckObjectName(uri, account+"/"+container, blobName); err != nil {
		return nil, "", err
	}
	store, ok := s.stores[container]
	if !ok {
		return nil, "", status.Errorf(codes.InvalidArgument, "asset URI %s: container %q is not one of the plugin's asset containers %v", uri, container, slices.Sorted(maps.Keys(s.stores)))
	}
	return store, blobName, nil
}

// assetStatus maps a FetchAsset error to a gRPC status. It differs from
// toStatus where FetchAsset's codes carry a narrower meaning: PermissionDenied
// is reserved for a refused snapshot location, so a storage authorization
// failure is Internal, and an absent container means the asset is absent.
// A 401 or 403 refuses the plugin's own identity, not the URL; atelet reads an
// asset anonymously after Internal only for gs:// URLs, which this plugin
// never reads.
func assetStatus(err error) error {
	switch {
	case errors.Is(err, blobstore.ErrContainerNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, blobstore.ErrPermission):
		return status.Errorf(codes.Internal, "the plugin's identity may not read the asset: %v", err)
	}
	return toStatus(err)
}

var _ objectstorev1.AssetProviderServer = (*AssetServer)(nil)
