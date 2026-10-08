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

//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"go.ate.dev/substrate/internal/plugins/azure-blob/internal/blobstore"
	objectstorev1 "go.ate.dev/substrate/pkg/proto/objectstorepb/v1"
)

func TestAzuriteRunEndToEnd(t *testing.T) {
	endpoint := os.Getenv("AZURITE_BLOB_ENDPOINT")
	account := os.Getenv("AZURITE_ACCOUNT_NAME")
	key := os.Getenv("AZURITE_ACCOUNT_KEY")
	if endpoint == "" || account == "" || key == "" {
		t.Fatal("AZURITE_BLOB_ENDPOINT, AZURITE_ACCOUNT_NAME, and AZURITE_ACCOUNT_KEY are required")
	}
	credential, err := azblob.NewSharedKeyCredential(account, key)
	if err != nil {
		t.Fatal(err)
	}
	serviceClient, err := service.NewClientWithSharedKeyCredential(endpoint, credential, nil)
	if err != nil {
		t.Fatal(err)
	}
	container := fmt.Sprintf("run-%d", time.Now().UnixNano())
	containerClient := serviceClient.NewContainerClient(container)
	if _, err := containerClient.Create(context.Background(), nil); err != nil {
		t.Fatalf("creating Azurite container: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = containerClient.Delete(ctx, nil)
	})

	root := t.TempDir()
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "destination")
	for _, dir := range []string{source, destination} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	want := bytes.Repeat([]byte("real-run-path"), 700000)
	if err := os.WriteFile(filepath.Join(source, "memory.img"), want, 0o600); err != nil {
		t.Fatal(err)
	}

	socket := testSocket(t, "node.sock")
	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() {
		errs <- run(ctx, []string{
			"node",
			"--socket=" + socket,
			"--account-url=" + endpoint,
			"--containers=" + container,
			"--prefix=run",
			"--root=" + root,
		}, func(accountURL string, containers []string) (map[string]blobstore.Store, error) {
			stores := map[string]blobstore.Store{}
			for _, name := range containers {
				store, err := blobstore.NewAzureWithSharedKey(accountURL, name, account, key)
				if err != nil {
					return nil, err
				}
				stores[name] = store
			}
			return stores, nil
		})
	}()
	conn := dialPlugin(t, socket)
	node := objectstorev1.NewNodeProviderClient(conn)
	callCtx, callCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer callCancel()
	uri := "azblob://" + container + "/actors/a/snapshots/one"
	if _, err := node.UploadSnapshot(callCtx, &objectstorev1.UploadSnapshotRequest{
		SnapshotUri: uri,
		LocalPath:   source,
		Files:       []string{"memory.img"},
	}); err != nil {
		t.Fatalf("UploadSnapshot through run: %v", err)
	}
	if _, err := node.FetchSnapshot(callCtx, &objectstorev1.FetchSnapshotRequest{
		SnapshotUri: uri,
		WritePath:   destination,
		Files:       []string{"memory.img"},
	}); err != nil {
		t.Fatalf("FetchSnapshot through run: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(destination, "memory.img"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("restored %d bytes, want %d", len(got), len(want))
	}

	cancel()
	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("run after cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not stop after cancellation")
	}
}
