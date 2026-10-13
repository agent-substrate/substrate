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

package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"go.ate.dev/substrate/internal/plugins/azure-blob/internal/blobstore"
	objectstorev1 "go.ate.dev/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// downloadCounter counts a real store's downloads.
type downloadCounter struct {
	blobstore.Store
	n atomic.Int64
}

func (d *downloadCounter) Download(ctx context.Context, name string) (blobstore.Download, error) {
	d.n.Add(1)
	return d.Store.Download(ctx, name)
}

// TestAzuriteFetchAsset serves AssetProvider on the real adapter: an asset
// uploaded the way an operator uploads one (a plain block blob, no substrate
// metadata) is fetched by its native URL, a snapshot the plugin itself wrote
// is refused by its native URL with its correct sha256 before any download,
// and Azure's missing-blob and missing-container responses map to NotFound.
func TestAzuriteFetchAsset(t *testing.T) {
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
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	assetsContainer := fmt.Sprintf("assets-%d", suffix)
	snapshotsContainer := fmt.Sprintf("snaps-%d", suffix)
	absentContainer := fmt.Sprintf("absent-%d", suffix)
	for _, name := range []string{assetsContainer, snapshotsContainer} {
		containerClient := serviceClient.NewContainerClient(name)
		if _, err := containerClient.Create(ctx, nil); err != nil {
			t.Fatalf("creating Azurite container %s: %v", name, err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, _ = containerClient.Delete(ctx, nil)
		})
	}
	stores := map[string]*downloadCounter{}
	for _, name := range []string{assetsContainer, snapshotsContainer, absentContainer} {
		azure, err := blobstore.NewAzureWithSharedKey(endpoint, name, account, key)
		if err != nil {
			t.Fatal(err)
		}
		stores[name] = &downloadCounter{Store: azure}
	}

	// An operator's asset: several of the SDK's download chunks long.
	kernel := bytes.Repeat([]byte("vmlinux!"), 3<<20)
	kernelSum := fmt.Sprintf("%x", sha256.Sum256(kernel))
	if _, err := serviceClient.NewContainerClient(assetsContainer).NewBlockBlobClient("kata-assets/vmlinux").UploadBuffer(ctx, kernel, nil); err != nil {
		t.Fatalf("uploading asset: %v", err)
	}

	// A snapshot the plugin writes, under its prefix "integration".
	root := t.TempDir()
	node, _ := serveIntegrationPlugin(t, map[string]blobstore.Store{snapshotsContainer: stores[snapshotsContainer]}, root)
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	pages := []byte("guest memory pages")
	if err := os.WriteFile(filepath.Join(source, "pages.img"), pages, 0o600); err != nil {
		t.Fatal(err)
	}
	snapshotURI := "azblob://" + snapshotsContainer + "/root/atespaces/team-a/actors/4805103b-9060-44bc-b54a-a220f9170186/snapshots/85594b93-62ef-4664-9346-2960302b58b5"
	if _, err := node.UploadSnapshot(ctx, &objectstorev1.UploadSnapshotRequest{SnapshotUri: snapshotURI, LocalPath: source, Files: []string{"pages.img"}}); err != nil {
		t.Fatalf("UploadSnapshot: %v", err)
	}
	snapshotBlobURL := endpoint + "/" + snapshotsContainer + "/integration/root/atespaces/team-a/actors/4805103b-9060-44bc-b54a-a220f9170186/snapshots/85594b93-62ef-4664-9346-2960302b58b5/pages.img"
	pagesSum := fmt.Sprintf("%x", sha256.Sum256(pages))
	if download, err := stores[snapshotsContainer].Store.Download(ctx, "integration/root/atespaces/team-a/actors/4805103b-9060-44bc-b54a-a220f9170186/snapshots/85594b93-62ef-4664-9346-2960302b58b5/pages.img"); err != nil {
		t.Fatalf("the snapshot blob the refusal names is not where the test expects it: %v", err)
	} else {
		download.Body.Close()
	}

	assetStores := map[string]blobstore.Store{}
	for name, store := range stores {
		assetStores[name] = store
	}
	staging := t.TempDir()
	client := serveAssetPlugin(t, endpoint, assetStores, root, staging)
	dst := filepath.Join(root, "asset")
	if err := os.WriteFile(dst, []byte(untouched), 0o600); err != nil {
		t.Fatal(err)
	}
	fetch := func(uri, sha string, maxBytes int64) error {
		_, err := client.FetchAsset(ctx, &objectstorev1.FetchAssetRequest{AssetUri: uri, Sha256: sha, WritePath: dst, MaxBytes: maxBytes})
		return err
	}

	assetURL := endpoint + "/" + assetsContainer + "/kata-assets/vmlinux"
	if err := fetch(snapshotBlobURL, pagesSum, 1<<20); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("FetchAsset(snapshot blob) = %v, want PermissionDenied", err)
	}
	if n := stores[snapshotsContainer].n.Load(); n != 0 {
		t.Fatalf("refusing the snapshot blob downloaded %d blobs, want 0", n)
	}
	for _, tc := range []struct {
		name     string
		uri, sha string
		maxBytes int64
		want     codes.Code
	}{
		{"sha256 mismatch", assetURL, pagesSum, 1 << 30, codes.FailedPrecondition},
		{"over max_bytes", assetURL, kernelSum, int64(len(kernel) - 1), codes.FailedPrecondition},
		{"absent blob", endpoint + "/" + assetsContainer + "/kata-assets/missing", kernelSum, 1 << 30, codes.NotFound},
		{"absent container", endpoint + "/" + absentContainer + "/kata-assets/vmlinux", kernelSum, 1 << 30, codes.NotFound},
	} {
		if err := fetch(tc.uri, tc.sha, tc.maxBytes); status.Code(err) != tc.want {
			t.Errorf("%s: FetchAsset = %v, want %s", tc.name, err, tc.want)
		}
	}
	if got, err := os.ReadFile(dst); err != nil || string(got) != untouched {
		t.Fatalf("failed fetches changed the write path: %q, %v", got, err)
	}
	if err := fetch(assetURL, kernelSum, int64(len(kernel))); err != nil {
		t.Fatalf("FetchAsset(asset) = %v, want success", err)
	}
	if got, err := os.ReadFile(dst); err != nil || !bytes.Equal(got, kernel) {
		t.Fatalf("write path holds %d bytes (err %v), want the %d-byte asset", len(got), err, len(kernel))
	}
	if entries, err := os.ReadDir(staging); err != nil || len(entries) != 0 {
		t.Fatalf("staging directory holds %v (err %v), want it empty", entries, err)
	}
}

func serveAssetPlugin(t *testing.T, accountURL string, stores map[string]blobstore.Store, root, staging string) objectstorev1.AssetProviderClient {
	t.Helper()
	server, err := NewAssetServer(accountURL, stores, root, staging, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	socketDir, err := os.MkdirTemp("", "azure-asset")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socket := filepath.Join(socketDir, "plugin.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	objectstorev1.RegisterAssetProviderServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(grpcServer.Stop)
	conn, err := grpc.NewClient("unix://"+socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return objectstorev1.NewAssetProviderClient(conn)
}
