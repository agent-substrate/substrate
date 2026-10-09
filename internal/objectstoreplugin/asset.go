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

package objectstoreplugin

import (
	"context"
	"io"
	"strings"

	"github.com/agent-substrate/substrate/pkg/assetfetch"
	"github.com/agent-substrate/substrate/pkg/objectstorage"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AssetPlugin serves AssetProvider on an object storage client. It reads
// gs:// and s3:// URIs as <bucket>/<object> on the configured backend, the way
// it reads snapshot URIs.
//
// It downloads each asset into stagingDir, which must be private to the
// plugin, and copies it to the caller's file only once its size and sha256
// check out (see assetfetch.Stager). The caller can therefore never read bytes
// it did not pin, such as a snapshot's.
type AssetPlugin struct {
	objectstorev1.UnimplementedAssetProviderServer

	client objectstorage.ObjectStorage
	stager *assetfetch.Stager
}

// NewAssetPlugin returns an AssetPlugin that writes caller files only below
// root and stages downloads in stagingDir, which must not be below root.
func NewAssetPlugin(client objectstorage.ObjectStorage, root, stagingDir string) (*AssetPlugin, error) {
	stager, err := assetfetch.NewStager(root, stagingDir)
	if err != nil {
		return nil, err
	}
	return &AssetPlugin{client: client, stager: stager}, nil
}

// FetchAsset downloads the object at asset_uri into the staging directory and,
// if it is at most max_bytes long and hashes to sha256, copies it into the
// existing file at write_path.
func (p *AssetPlugin) FetchAsset(ctx context.Context, req *objectstorev1.FetchAssetRequest) (*objectstorev1.FetchAssetResponse, error) {
	bucket, object, err := parseAssetURI(req.GetAssetUri())
	if err != nil {
		return nil, err
	}
	open := func(ctx context.Context) (io.ReadCloser, error) {
		return p.client.GetObject(ctx, bucket, object)
	}
	if err := p.stager.Fetch(ctx, req, req.GetAssetUri(), open, toStatus); err != nil {
		return nil, err
	}
	return &objectstorev1.FetchAssetResponse{}, nil
}

// parseAssetURI accepts gs://<bucket>/<object> and s3://<bucket>/<object>
// whose object name is in canonical form and is not stored under a snapshot
// location, and returns the bucket and object name.
func parseAssetURI(uri string) (bucket, object string, err error) {
	u, err := assetfetch.ParseURI(uri)
	if err != nil {
		return "", "", err
	}
	if u.Scheme != "gs" && u.Scheme != "s3" {
		return "", "", status.Errorf(codes.InvalidArgument, "asset URI %s: scheme %q is not served by this plugin (want gs or s3)", uri, u.Scheme)
	}
	object = strings.TrimPrefix(u.Path, "/")
	if u.Host == "" || object == "" {
		return "", "", status.Errorf(codes.InvalidArgument, "asset URI %s must name a bucket and an object", uri)
	}
	if err := assetfetch.CheckObjectName(uri, u.Scheme+"://"+u.Host, object); err != nil {
		return "", "", err
	}
	return u.Host, object, nil
}
