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

// Package objectstoretest provides an in-memory objectstore.Store for tests
// that need to observe which external snapshots a flow created and released.
package objectstoretest

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/resources"
)

// Fake is an in-memory object store. It is safe for concurrent use, as the
// prefix helpers operate on several objects at once.
type Fake struct {
	// OnList, OnDelete and OnCopy, when set, run before the operation they name
	// and fail it if they return an error. Set them before the Fake is handed
	// to the code under test.
	OnList   func(bucket, prefix string) error
	OnDelete func(bucket, object string) error
	OnCopy   func(srcBucket, srcObject, dstBucket, dstObject string) error

	mu      sync.Mutex
	objects map[string]string
}

var _ objectstore.Store = (*Fake)(nil)

// New returns an empty Fake.
func New() *Fake {
	return &Fake{objects: map[string]string{}}
}

func (f *Fake) List(_ context.Context, bucket, prefix string) ([]string, error) {
	if f.OnList != nil {
		if err := f.OnList(bucket, prefix); err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var objects []string
	for key := range f.objects {
		if keyBucket, object, found := strings.Cut(key, "/"); found && keyBucket == bucket && strings.HasPrefix(object, prefix) {
			objects = append(objects, object)
		}
	}
	// A real backend lists in lexicographic order, and a stable order keeps
	// assertions on the call sequence meaningful.
	slices.Sort(objects)
	return objects, nil
}

func (f *Fake) Delete(_ context.Context, bucket, object string) error {
	if f.OnDelete != nil {
		if err := f.OnDelete(bucket, object); err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, key(bucket, object))
	return nil
}

func (f *Fake) Copy(_ context.Context, srcBucket, srcObject, dstBucket, dstObject string) error {
	if f.OnCopy != nil {
		if err := f.OnCopy(srcBucket, srcObject, dstBucket, dstObject); err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	content, ok := f.objects[key(srcBucket, srcObject)]
	if !ok {
		return fmt.Errorf("no such object: %s", key(srcBucket, srcObject))
	}
	f.objects[key(dstBucket, dstObject)] = content
	return nil
}

// Put writes one object, creating its bucket implicitly.
func (f *Fake) Put(bucket, object, content string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key(bucket, object)] = content
}

// PutSnapshot writes named objects into the external snapshot at uri, each
// holding its own name as content so a copy can be traced back to its source.
func (f *Fake) PutSnapshot(t *testing.T, uri resources.SnapshotURI, names ...string) {
	t.Helper()
	if err := f.WriteSnapshot(uri.String(), names...); err != nil {
		t.Fatalf("WriteSnapshot(%s) = %v", uri, err)
	}
}

// WriteSnapshot is PutSnapshot for callers outside a test body — a fake atelet
// serving a checkpoint has no *testing.T to fail on, and holds the destination
// as the unparsed URI it was sent.
func (f *Fake) WriteSnapshot(snapshotURI string, names ...string) error {
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		return fmt.Errorf("while parsing the snapshot URI %q: %w", snapshotURI, err)
	}
	bucket, prefix, err := objectstore.BucketPrefix(uri.Prefix())
	if err != nil {
		return err
	}
	for _, name := range names {
		f.Put(bucket, prefix+name, name)
	}
	return nil
}

// Snapshot returns the names, relative to uri, of the objects the external
// snapshot at uri is made of. An empty result means the snapshot is not there.
func (f *Fake) Snapshot(t *testing.T, uri resources.SnapshotURI) []string {
	t.Helper()
	return f.Prefix(t, uri.Prefix())
}

// Prefix returns the names, relative to prefix, of every object below it. It is
// how a test asserts on a whole owner's subtree: that deleting one resource
// collected all of its objects, or left another's alone.
func (f *Fake) Prefix(t *testing.T, storagePrefix resources.StoragePrefix) []string {
	t.Helper()
	bucket, prefix, err := objectstore.BucketPrefix(storagePrefix)
	if err != nil {
		t.Fatalf("BucketPrefix(%s) = %v", storagePrefix, err)
	}
	objects, err := f.List(t.Context(), bucket, prefix)
	if err != nil {
		t.Fatalf("List(%s, %s) = %v", bucket, prefix, err)
	}
	names := make([]string, 0, len(objects))
	for _, object := range objects {
		names = append(names, strings.TrimPrefix(object, prefix))
	}
	return names
}

// Objects returns every object in the store as "bucket/object", sorted.
func (f *Fake) Objects() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func key(bucket, object string) string { return bucket + "/" + object }
