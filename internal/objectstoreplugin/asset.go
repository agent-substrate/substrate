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
	"fmt"
	"io"
	"strings"

	"github.com/agent-substrate/substrate/pkg/assetfetch"
	"github.com/agent-substrate/substrate/pkg/objectstorage"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AssetPlugin serves AssetProvider on an object storage client. It reads
// URIs of its backend's scheme (gs:// on GCS, s3:// on S3) as
// <bucket>/<object>, and returns InvalidArgument for any other scheme without
// reading anything: a gs:// bucket name read on S3 names someone else's
// bucket, or none. atelet then reads a gs:// asset anonymously.
//
// It downloads each asset into stagingDir, which must be private to the
// plugin, and copies it to the caller's file only once its size and sha256
// check out (see assetfetch.Stager). The caller can therefore never read bytes
// it did not pin, such as a snapshot's.
type AssetPlugin struct {
	objectstorev1.UnimplementedAssetProviderServer

	client objectstorage.ObjectStorage
	scheme string
	stager *assetfetch.Stager
}

// NewAssetPlugin returns an AssetPlugin that serves URIs of scheme ("gs" or
// "s3", client's backend) from client, writes caller files only below root and
// stages downloads in stagingDir, which must not be below root, holding at
// most stagingCapacity bytes there at once.
func NewAssetPlugin(client objectstorage.ObjectStorage, scheme, root, stagingDir string, stagingCapacity int64) (*AssetPlugin, error) {
	if scheme != "gs" && scheme != "s3" {
		return nil, fmt.Errorf("asset URI scheme %q is not gs or s3", scheme)
	}
	stager, err := assetfetch.NewStager(root, stagingDir, stagingCapacity)
	if err != nil {
		return nil, err
	}
	return &AssetPlugin{client: client, scheme: scheme, stager: stager}, nil
}

// FetchAsset downloads the object at asset_uri into the staging directory and,
// if it is at most max_bytes long and hashes to sha256, copies it into the
// existing file at write_path (see assetfetch.Stager.Fetch).
func (p *AssetPlugin) FetchAsset(ctx context.Context, req *objectstorev1.FetchAssetRequest) (*objectstorev1.FetchAssetResponse, error) {
	bucket, object, err := parseAssetURI(req.GetAssetUri(), p.scheme)
	if err != nil {
		return nil, err
	}
	open := func(ctx context.Context) (io.ReadCloser, int64, error) {
		rc, err := p.client.GetObject(ctx, bucket, object)
		if err != nil {
			return nil, 0, err
		}
		size, ok := objectstorage.ObjectSize(rc)
		if !ok {
			size = -1
		}
		return rc, size, nil
	}
	if err := p.stager.Fetch(ctx, req, req.GetAssetUri(), open, toStatus); err != nil {
		return nil, err
	}
	return &objectstorev1.FetchAssetResponse{}, nil
}

// parseAssetURI accepts <scheme>://<bucket>/<object> whose object name is in
// canonical form and is not stored under a snapshot location, and returns the
// bucket and object name.
func parseAssetURI(uri, scheme string) (bucket, object string, err error) {
	u, err := assetfetch.ParseURI(uri)
	if err != nil {
		return "", "", err
	}
	if u.Scheme != scheme {
		return "", "", status.Errorf(codes.InvalidArgument, "asset URI %s: scheme %q is not served by this plugin, whose storage backend serves %s://", uri, u.Scheme, scheme)
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
