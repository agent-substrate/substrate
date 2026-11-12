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
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	objectstorev1 "go.ate.dev/substrate/api/objectstore/v1"
	"go.ate.dev/substrate/internal/objectstore"
	"go.ate.dev/substrate/internal/resources"
)

// ObjectStoreMethod is the gRPC full name of a ControlProvider RPC ate-api calls
// on the object-store plugin, which a fault can be injected into.
type ObjectStoreMethod string

const (
	ObjectStoreCleanupSnapshot = ObjectStoreMethod(objectstorev1.ControlProvider_CleanupSnapshot_FullMethodName)
	ObjectStoreCopySnapshot    = ObjectStoreMethod(objectstorev1.ControlProvider_CopySnapshot_FullMethodName)
)

// ErrStorageFailed is object storage failing a request, as the plugin reports
// a failure it does not recognize.
var ErrStorageFailed = status.Error(codes.Internal, "harness: storage request failed")

// ObjectStore is the in-memory blob storage shared by ate-api and every fake
// atelet in a test, as one bucket is shared in a real cluster. ate-api reaches
// it through the object-store plugin's ControlProvider service.
type ObjectStore struct {
	fake *fakeServer[ObjectStoreMethod]

	mu         sync.Mutex
	objectKeys map[string]bool
}

// objectStoreServer serves an ObjectStore to ate-api as the object-store
// plugin's ControlProvider.
type objectStoreServer struct {
	objectstorev1.UnimplementedControlProviderServer
	store *ObjectStore
}

func newObjectStore() *ObjectStore {
	return &ObjectStore{
		fake:       newFakeServer(ObjectStoreCleanupSnapshot, ObjectStoreCopySnapshot),
		objectKeys: map[string]bool{},
	}
}

// On returns the Fault deciding how ate-api's calls to method behave. The fake
// atelets' uploads are not faulted.
func (s *ObjectStore) On(method ObjectStoreMethod) *Fault {
	return s.fake.on(method)
}

// grpcServer returns a gRPC server of the ControlProvider s serves to
// ate-api, which applies s's faults and records its calls. Tests hold only
// the ObjectStore, so they reach its server side only through ate-api.
func (s *ObjectStore) grpcServer() *grpc.Server {
	srv := s.fake.newServer()
	objectstorev1.RegisterControlProviderServer(srv, &objectStoreServer{store: s})
	return srv
}

// RecordCalls starts recording the calls ate-api makes to s from now on.
func (s *ObjectStore) RecordCalls() *Recording[ObjectStoreMethod] {
	return s.fake.record()
}

// Objects returns every object as "bucket/object", sorted.
func (s *ObjectStore) Objects() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Sorted(maps.Keys(s.objectKeys))
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
	return s.namesWithPrefix(bucket, prefix), nil
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
	s.deleteWithPrefix(bucket, prefix)
	return nil
}

// CleanupSnapshot implements ControlProvider: it deletes every object below
// snapshot_uri. Deleting an absent snapshot succeeds.
func (o *objectStoreServer) CleanupSnapshot(_ context.Context, req *objectstorev1.CleanupSnapshotRequest) (*objectstorev1.CleanupSnapshotResponse, error) {
	s := o.store
	bucket, prefix, err := storagePrefix(req.GetSnapshotUri())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteWithPrefix(bucket, prefix)
	return &objectstorev1.CleanupSnapshotResponse{}, nil
}

// CopySnapshot implements ControlProvider: it copies every object below
// src_uri to dst_uri. It fails with FAILED_PRECONDITION if src_uri holds no
// objects.
func (o *objectStoreServer) CopySnapshot(_ context.Context, req *objectstorev1.CopySnapshotRequest) (*objectstorev1.CopySnapshotResponse, error) {
	s := o.store
	srcBucket, srcPrefix, err := storagePrefix(req.GetSrcUri())
	if err != nil {
		return nil, err
	}
	dstBucket, dstPrefix, err := storagePrefix(req.GetDstUri())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	objects := s.namesWithPrefix(srcBucket, srcPrefix)
	if len(objects) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "no objects below %s", req.GetSrcUri())
	}
	for _, name := range objects {
		s.objectKeys[objectKey(dstBucket, dstPrefix+name)] = true
	}
	return &objectstorev1.CopySnapshotResponse{}, nil
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

// namesWithPrefix returns the names of the objects in bucket that start with
// prefix, with prefix trimmed and sorted as a real backend lists them. s.mu
// must be held.
func (s *ObjectStore) namesWithPrefix(bucket, prefix string) []string {
	keyPrefix := objectKey(bucket, prefix)
	var names []string
	for k := range s.objectKeys {
		if name, ok := strings.CutPrefix(k, keyPrefix); ok {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// deleteWithPrefix deletes the objects in bucket that start with prefix. s.mu
// must be held.
func (s *ObjectStore) deleteWithPrefix(bucket, prefix string) {
	keyPrefix := objectKey(bucket, prefix)
	maps.DeleteFunc(s.objectKeys, func(k string, _ bool) bool { return strings.HasPrefix(k, keyPrefix) })
}

// storagePrefix returns the bucket and object prefix of a ControlProvider
// request's URI, failing with INVALID_ARGUMENT as the plugin does.
func storagePrefix(uri string) (bucket, prefix string, err error) {
	parsed, err := resources.ParseStoragePrefix(uri)
	if err != nil {
		return "", "", status.Error(codes.InvalidArgument, err.Error())
	}
	bucket, prefix, err = objectstore.BucketPrefix(parsed)
	if err != nil {
		return "", "", status.Error(codes.InvalidArgument, err.Error())
	}
	return bucket, prefix, nil
}

func snapshotPrefix(snapshotURI string) (bucket, prefix string, err error) {
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		return "", "", fmt.Errorf("while parsing the snapshot URI %q: %w", snapshotURI, err)
	}
	return objectstore.BucketPrefix(uri.Prefix())
}

func objectKey(bucket, object string) string { return bucket + "/" + object }
