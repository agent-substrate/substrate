// Copyright 2026 The Agent Substrate Authors
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
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"cloud.google.com/go/storage"

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

// ObjectStoreListRequest holds the arguments of a List, as its RPC records
// them.
type ObjectStoreListRequest struct {
	Bucket, Prefix string
}

// ObjectStoreDeleteRequest holds the arguments of a Delete, as its RPC records
// them.
type ObjectStoreDeleteRequest struct {
	Bucket, Object string
}

// ObjectStoreCopyRequest holds the arguments of a Copy, as its RPC records
// them.
type ObjectStoreCopyRequest struct {
	SrcBucket, SrcObject, DstBucket, DstObject string
}

var (
	// ErrStorageFailed is object storage failing a request.
	ErrStorageFailed = errors.New("harness: storage request failed")
	// ErrBucketNotExist is object storage reporting that the bucket is gone,
	// as the GCS client does.
	ErrBucketNotExist = fmt.Errorf("harness: %w", storage.ErrBucketNotExist)
)

// ObjectStore is the in-memory blob storage shared by ate-api and every fake
// atelet in a test, as one bucket is shared in a real cluster.
type ObjectStore struct {
	faults map[ObjectStoreOp]*Fault
	rpcs   rpcLog[ObjectStoreOp]

	mu         sync.Mutex
	objectKeys map[string]bool
}

// objectStoreServer serves an ObjectStore to ate-api's object-store plugin.
type objectStoreServer struct {
	store *ObjectStore
}

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

// server returns the object storage s serves to ate-api. Tests hold only the
// ObjectStore, so they reach its server side only through ate-api.
func (s *ObjectStore) server() objectstore.Store {
	return objectStoreServer{store: s}
}

// RecordRPCs starts recording the calls ate-api makes to s from now on.
func (s *ObjectStore) RecordRPCs() *RPCRecording[ObjectStoreOp] {
	return s.rpcs.record()
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
func (o objectStoreServer) List(ctx context.Context, bucket, prefix string) ([]string, error) {
	s := o.store
	s.rpcs.add(ObjectStoreList, ObjectStoreListRequest{Bucket: bucket, Prefix: prefix})
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
func (o objectStoreServer) Delete(ctx context.Context, bucket, object string) error {
	s := o.store
	s.rpcs.add(ObjectStoreDelete, ObjectStoreDeleteRequest{Bucket: bucket, Object: object})
	out := s.faults[ObjectStoreDelete].decide(ctx)
	if out.apply {
		s.mu.Lock()
		delete(s.objectKeys, objectKey(bucket, object))
		s.mu.Unlock()
	}
	return out.result(ctx)
}

// Copy implements objectstore.Store.
func (o objectStoreServer) Copy(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) error {
	s := o.store
	s.rpcs.add(ObjectStoreCopy, ObjectStoreCopyRequest{SrcBucket: srcBucket, SrcObject: srcObject, DstBucket: dstBucket, DstObject: dstObject})
	out := s.faults[ObjectStoreCopy].decide(ctx)
	if out.apply {
		s.mu.Lock()
		found := s.objectKeys[objectKey(srcBucket, srcObject)]
		if found {
			s.objectKeys[objectKey(dstBucket, dstObject)] = true
		}
		s.mu.Unlock()
		if !found {
			return fmt.Errorf("no such object: %s", objectKey(srcBucket, srcObject))
		}
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
