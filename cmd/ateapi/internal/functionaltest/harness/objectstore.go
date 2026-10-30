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
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	objectstorev1 "go.ate.dev/substrate/api/objectstore/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/functionaltest/harness/grpcfake"
	"go.ate.dev/substrate/internal/objectstore"
	"go.ate.dev/substrate/internal/objectstore/objectstoretest"
	"go.ate.dev/substrate/internal/objectstoreplugin"
	"go.ate.dev/substrate/internal/resources"
)

// ObjectStoreMethod is the gRPC full name of a ControlProvider RPC ate-api
// calls on the object-store plugin, which a fault can be injected into.
type ObjectStoreMethod string

const (
	ObjectStoreCleanupSnapshot = ObjectStoreMethod(objectstorev1.ControlProvider_CleanupSnapshot_FullMethodName)
	ObjectStoreCopySnapshot    = ObjectStoreMethod(objectstorev1.ControlProvider_CopySnapshot_FullMethodName)
)

// ErrStorageFailed is object storage failing a request, as the plugin reports
// a failure it does not recognize.
var ErrStorageFailed = status.New(codes.Internal, "harness: storage request failed")

// ObjectStore is the in-memory blob storage shared by ate-api and every fake
// atelet in a test, as one bucket is shared in a real cluster. ate-api reaches
// it through the object-store plugin's ControlProvider service.
type ObjectStore struct {
	fake  *grpcfake.Service[ObjectStoreMethod]
	store *objectstoretest.Fake
}

func newObjectStore() *ObjectStore {
	return &ObjectStore{
		fake:  grpcfake.NewService(ObjectStoreCleanupSnapshot, ObjectStoreCopySnapshot),
		store: objectstoretest.New(),
	}
}

// On returns the Fault deciding how ate-api's calls to method behave. The fake
// atelets' uploads are not faulted.
func (s *ObjectStore) On(method ObjectStoreMethod) *grpcfake.Fault {
	return s.fake.On(method)
}

// grpcServer returns a gRPC server of the object-store plugin's
// ControlProvider on s, which applies s's faults and records its calls.
// Tests hold only the ObjectStore, so they reach its server side only through
// ate-api.
func (s *ObjectStore) grpcServer() *grpc.Server {
	srv := s.fake.NewServer()
	objectstorev1.RegisterControlProviderServer(srv, objectstoreplugin.NewControlPlugin(s.store))
	return srv
}

// RecordCalls starts recording the calls ate-api makes to s from now on.
func (s *ObjectStore) RecordCalls() *grpcfake.Recording[ObjectStoreMethod] {
	return s.fake.Record()
}

// Objects returns every object as "bucket/object", sorted.
func (s *ObjectStore) Objects() []string {
	return s.store.Objects()
}

// Snapshot returns the names, relative to snapshotURI, of the objects the
// external snapshot there is made of. Empty means the snapshot is not there.
func (s *ObjectStore) Snapshot(snapshotURI string) ([]string, error) {
	prefix, err := snapshotPrefix(snapshotURI)
	if err != nil {
		return nil, err
	}
	bucket, object, err := objectstore.BucketPrefix(prefix)
	if err != nil {
		return nil, err
	}
	objects, err := s.store.List(context.Background(), bucket, object)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(objects))
	for _, o := range objects {
		names = append(names, strings.TrimPrefix(o, object))
	}
	return names, nil
}

// DeleteSnapshot removes every object of the external snapshot at snapshotURI,
// as if it had been lost from blob storage.
func (s *ObjectStore) DeleteSnapshot(snapshotURI string) error {
	prefix, err := snapshotPrefix(snapshotURI)
	if err != nil {
		return err
	}
	return objectstore.DeletePrefix(context.Background(), s.store, prefix)
}

// writeSnapshot records the objects of the external snapshot at snapshotURI,
// as an atelet checkpoint or upload writes them.
func (s *ObjectStore) writeSnapshot(snapshotURI string, names ...string) error {
	return s.store.WriteSnapshot(snapshotURI, names...)
}

func snapshotPrefix(snapshotURI string) (resources.StoragePrefix, error) {
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		return resources.StoragePrefix{}, fmt.Errorf("while parsing the snapshot URI %q: %w", snapshotURI, err)
	}
	return uri.Prefix(), nil
}
