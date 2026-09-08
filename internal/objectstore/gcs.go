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

package objectstore

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type gcsStore struct {
	client *storage.Client
}

// NewGCS returns a Store backed by Google Cloud Storage.
func NewGCS(client *storage.Client) Store {
	return &gcsStore{client: client}
}

func (g *gcsStore) List(ctx context.Context, bucket, prefix string) ([]string, error) {
	query := &storage.Query{Prefix: prefix}
	// Only the name is ever used, and asking for less makes GCS send less.
	if err := query.SetAttrSelection([]string{"Name"}); err != nil {
		return nil, fmt.Errorf("while selecting object attributes: %w", err)
	}
	it := g.client.Bucket(bucket).Objects(ctx, query)
	var objects []string
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return objects, nil
		}
		if err != nil {
			return nil, fmt.Errorf("while listing objects under gs://%s/%s: %w", bucket, prefix, err)
		}
		objects = append(objects, attrs.Name)
	}
}

func (g *gcsStore) Delete(ctx context.Context, bucket, object string) error {
	err := g.client.Bucket(bucket).Object(object).Delete(ctx)
	// An object that is already gone is the state this asks for.
	if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("while deleting gs://%s/%s: %w", bucket, object, err)
	}
	return nil
}

func (g *gcsStore) Copy(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) error {
	src := g.client.Bucket(srcBucket).Object(srcObject)
	dst := g.client.Bucket(dstBucket).Object(dstObject)
	// Copier.Run drives GCS's rewrite API, following the rewrite token until
	// the copy completes. The bytes move inside GCS whatever the object's size,
	// so a multi-gigabyte memory image never reaches this process.
	if _, err := dst.CopierFrom(src).Run(ctx); err != nil {
		return fmt.Errorf("while copying gs://%s/%s to gs://%s/%s: %w", srcBucket, srcObject, dstBucket, dstObject, err)
	}
	return nil
}
