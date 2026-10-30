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

package harness

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"go.ate.dev/substrate/internal/objectstore"
	"go.ate.dev/substrate/internal/resources"
)

// ObjectStoreOp names an object storage operation ate-api calls, which a
// fault can be injected into.
type ObjectStoreOp string

const (
	ObjectStoreList   ObjectStoreOp = "List"
	ObjectStoreDelete ObjectStoreOp = "Delete"
	ObjectStoreCopy   ObjectStoreOp = "Copy"
)

// ObjectStore is the in-memory blob storage shared by ate-api and every fake
// atelet in a test, as one bucket is shared in a real cluster.
type ObjectStore struct {
	faults map[ObjectStoreOp]*Fault

	mu         sync.Mutex
	objectKeys map[string]bool
}

var _ objectstore.Store = (*ObjectStore)(nil)

func newObjectStore() *ObjectStore {
	return &ObjectStore{
		faults:     map[ObjectStoreOp]*Fault{ObjectStoreList: {}, ObjectStoreDelete: {}, ObjectStoreCopy: {}},
		objectKeys: map[string]bool{},
	}
}

// On returns the Fault deciding how ate-api's calls to op behave. The fake
// atelets' uploads are not faulted.
func (s *ObjectStore) On(op ObjectStoreOp) *Fault {
	return s.faults[op]
}

// Objects returns every object as "bucket/object", sorted.
func (s *ObjectStore) Objects() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.objectKeys))
	for k := range s.objectKeys {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// Snapshot returns the names, relative to snapshotURI, of the objects the
// external snapshot there is made of. Empty means the snapshot is not there.
func (s *ObjectStore) Snapshot(snapshotURI string) ([]string, error) {
	bucket, prefix, err := snapshotPrefix(snapshotURI)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listLocked(bucket, prefix, true), nil
}

// DeleteSnapshot removes every object of the external snapshot at snapshotURI,
// as if it had been lost from blob storage.
func (s *ObjectStore) DeleteSnapshot(snapshotURI string) error {
	bucket, prefix, err := snapshotPrefix(snapshotURI)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, object := range s.listLocked(bucket, prefix, false) {
		delete(s.objectKeys, objectKey(bucket, object))
	}
	return nil
}

// List implements objectstore.Store.
func (s *ObjectStore) List(ctx context.Context, bucket, prefix string) ([]string, error) {
	out := s.faults[ObjectStoreList].decide(ctx)
	var objects []string
	if out.apply {
		s.mu.Lock()
		objects = s.listLocked(bucket, prefix, false)
		s.mu.Unlock()
	}
	if err := out.result(ctx); err != nil {
		return nil, err
	}
	return objects, nil
}

// Delete implements objectstore.Store. Deleting a missing object succeeds.
func (s *ObjectStore) Delete(ctx context.Context, bucket, object string) error {
	out := s.faults[ObjectStoreDelete].decide(ctx)
	if out.apply {
		s.mu.Lock()
		delete(s.objectKeys, objectKey(bucket, object))
		s.mu.Unlock()
	}
	return out.result(ctx)
}

// Copy implements objectstore.Store.
func (s *ObjectStore) Copy(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) error {
	out := s.faults[ObjectStoreCopy].decide(ctx)
	if out.apply {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.objectKeys[objectKey(srcBucket, srcObject)] {
			return fmt.Errorf("no such object: %s", objectKey(srcBucket, srcObject))
		}
		s.objectKeys[objectKey(dstBucket, dstObject)] = true
	}
	return out.result(ctx)
}

// writeSnapshot records the objects of the external snapshot at snapshotURI,
// as an atelet checkpoint or upload writes them.
func (s *ObjectStore) writeSnapshot(snapshotURI string, names ...string) error {
	bucket, prefix, err := snapshotPrefix(snapshotURI)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range names {
		s.objectKeys[objectKey(bucket, prefix+name)] = true
	}
	return nil
}

// listLocked returns the objects below prefix in bucket, sorted as a real
// backend lists them. relative trims the prefix from each name.
func (s *ObjectStore) listLocked(bucket, prefix string, relative bool) []string {
	var objects []string
	for k := range s.objectKeys {
		keyBucket, object, _ := strings.Cut(k, "/")
		if keyBucket != bucket || !strings.HasPrefix(object, prefix) {
			continue
		}
		if relative {
			object = strings.TrimPrefix(object, prefix)
		}
		objects = append(objects, object)
	}
	slices.Sort(objects)
	return objects
}

func snapshotPrefix(snapshotURI string) (bucket, prefix string, err error) {
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		return "", "", fmt.Errorf("while parsing the snapshot URI %q: %w", snapshotURI, err)
	}
	return objectstore.BucketPrefix(uri.Prefix())
}

func objectKey(bucket, object string) string { return bucket + "/" + object }
