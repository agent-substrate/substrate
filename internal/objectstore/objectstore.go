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

// Package objectstore manages the existence of the objects an external
// snapshot is made of: listing them, copying them, and deleting them once
// nothing owns them any more.
//
// It is deliberately separate from atelet's ategcs, which reads and writes
// snapshot content. The control plane never handles those bytes: it copies
// server-side and deletes by name, so a multi-gigabyte memory image never
// transits ate-api.
package objectstore

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/agent-substrate/substrate/internal/resources"
	"golang.org/x/sync/errgroup"
)

// prefixConcurrency is how many per-object operations of one prefix run at
// once. A snapshot is a handful of objects, so this is about not serializing
// their round trips rather than about throughput.
const prefixConcurrency = 8

// Store is the object-storage surface the control plane needs. Every method
// addresses objects by name only.
type Store interface {
	// List returns the full names of the objects under prefix in bucket.
	List(ctx context.Context, bucket, prefix string) ([]string, error)
	// Delete removes one object. Deleting an object that is already gone
	// succeeds, so a retry over a partly-collected snapshot finishes cleanly.
	Delete(ctx context.Context, bucket, object string) error
	// Copy copies one object server-side. The destination is overwritten if it
	// exists, so a retry re-copying it is harmless.
	Copy(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) error
}

// DeletePrefix removes every object under uri: one external snapshot, or
// every snapshot an owner holds. A prefix with no objects left is already
// collected, so it succeeds.
func DeletePrefix(ctx context.Context, s Store, uri resources.StoragePrefix) error {
	bucket, prefix, err := BucketPrefix(uri)
	if err != nil {
		return err
	}
	objects, err := s.List(ctx, bucket, prefix)
	if err != nil {
		return fmt.Errorf("while listing %s: %w", uri, err)
	}
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(prefixConcurrency)
	for _, object := range objects {
		group.Go(func() error {
			if err := s.Delete(ctx, bucket, object); err != nil {
				return fmt.Errorf("while deleting %s from bucket %s: %w", object, bucket, err)
			}
			return nil
		})
	}
	return group.Wait()
}

// CopyPrefix copies every object of the external snapshot at src to dst,
// keeping each object's name relative to the prefix. The destination is
// deterministic, so a retry of a partly-completed copy overwrites what it
// already wrote instead of leaving a second copy behind.
func CopyPrefix(ctx context.Context, s Store, src, dst resources.StoragePrefix) error {
	srcBucket, srcPrefix, err := BucketPrefix(src)
	if err != nil {
		return err
	}
	dstBucket, dstPrefix, err := BucketPrefix(dst)
	if err != nil {
		return err
	}
	objects, err := s.List(ctx, srcBucket, srcPrefix)
	if err != nil {
		return fmt.Errorf("while listing %s: %w", src, err)
	}
	// Unlike a delete, an empty source is a failure: copying nothing would
	// leave the destination naming an external snapshot that cannot be
	// restored from.
	if len(objects) == 0 {
		return fmt.Errorf("external snapshot %s has no objects to copy", src)
	}
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(prefixConcurrency)
	for _, object := range objects {
		dstObject := dstPrefix + strings.TrimPrefix(object, srcPrefix)
		group.Go(func() error {
			if err := s.Copy(ctx, srcBucket, object, dstBucket, dstObject); err != nil {
				return fmt.Errorf("while copying %s to %s: %w", object, dstObject, err)
			}
			return nil
		})
	}
	return group.Wait()
}

// BucketPrefix splits a storage prefix into the bucket and the object-name
// prefix its objects share. The prefix keeps its trailing separator so that it
// cannot match a sibling whose name it happens to prefix.
func BucketPrefix(uri resources.StoragePrefix) (string, string, error) {
	if uri.IsZero() {
		return "", "", fmt.Errorf("empty storage prefix")
	}
	parsed, err := url.Parse(uri.String())
	if err != nil {
		return "", "", fmt.Errorf("while parsing storage prefix %s: %w", uri, err)
	}
	if parsed.Host == "" {
		return "", "", fmt.Errorf("storage prefix %s has no bucket", uri)
	}
	return parsed.Host, strings.TrimPrefix(parsed.Path, "/") + "/", nil
}
