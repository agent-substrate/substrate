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
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/agent-substrate/substrate/internal/plugins/azure-blob/internal/blobstore"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const largeIntegrationFileSize = 320 * 1024 * 1024

type failStageStore struct {
	blobstore.Store
	failAt int64
	calls  atomic.Int64
}

func (s *failStageStore) StageBlock(ctx context.Context, name, blockID string, body io.ReadSeeker, checksum []byte) error {
	if s.calls.Add(1) == s.failAt {
		return blobstore.ErrUnavailable
	}
	return s.Store.StageBlock(ctx, name, blockID, body, checksum)
}

func TestAzuriteGRPCEndToEnd(t *testing.T) {
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
	suffix := time.Now().UnixNano()
	containerName := fmt.Sprintf("snapshot-%d", suffix)
	tagsContainer := fmt.Sprintf("tags-%d", suffix)
	for _, name := range []string{containerName, tagsContainer} {
		containerClient := serviceClient.NewContainerClient(name)
		if _, err := containerClient.Create(context.Background(), nil); err != nil {
			t.Fatalf("creating Azurite container %s: %v", name, err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, _ = containerClient.Delete(ctx, nil)
		})
	}
	azure, err := blobstore.NewAzureWithSharedKey(endpoint, containerName, account, key)
	if err != nil {
		t.Fatal(err)
	}
	tags, err := blobstore.NewAzureWithSharedKey(endpoint, tagsContainer, account, key)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	failing := &failStageStore{Store: azure, failAt: 2}
	node, control := serveIntegrationPlugin(t, map[string]blobstore.Store{containerName: failing, tagsContainer: tags}, root)
	snapshotURI := "azblob://" + containerName + "/large"
	source := filepath.Join(root, "source")
	dest := filepath.Join(root, "dest")
	for _, dir := range []string{source, dest} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	largePath := filepath.Join(source, "memory.img")
	file, err := os.Create(largePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(largeIntegrationFileSize); err != nil {
		file.Close()
		t.Fatal(err)
	}
	for offset := int64(0); offset < largeIntegrationFileSize; offset += minBlockSize {
		pattern := sha256.Sum256([]byte(fmt.Sprintf("block-%d", offset/minBlockSize)))
		if _, err := file.WriteAt(pattern[:], offset); err != nil {
			file.Close()
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	upload := &objectstorev1.UploadSnapshotRequest{
		SnapshotUri: snapshotURI, LocalPath: source, Files: []string{"memory.img"},
	}
	if _, err := node.UploadSnapshot(context.Background(), upload); status.Code(err) != codes.Unavailable {
		t.Fatalf("partial UploadSnapshot code = %v, want Unavailable: %v", status.Code(err), err)
	}
	failing.failAt = 0
	if _, err := node.UploadSnapshot(context.Background(), upload); err != nil {
		t.Fatalf("retried large UploadSnapshot: %v", err)
	}
	if _, err := node.UploadSnapshot(context.Background(), upload); err != nil {
		t.Fatalf("idempotent large UploadSnapshot: %v", err)
	}
	fetch := &objectstorev1.FetchSnapshotRequest{
		SnapshotUri: snapshotURI, WritePath: dest, Files: []string{"memory.img"},
	}
	if _, err := node.FetchSnapshot(context.Background(), fetch); err != nil {
		t.Fatalf("FetchSnapshot: %v", err)
	}
	if _, err := node.FetchSnapshot(context.Background(), fetch); err != nil {
		t.Fatalf("idempotent FetchSnapshot: %v", err)
	}
	info, err := os.Stat(filepath.Join(dest, "memory.img"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != largeIntegrationFileSize {
		t.Fatalf("fetched size = %d, want %d", info.Size(), largeIntegrationFileSize)
	}
	assertFileHash(t, largePath, filepath.Join(dest, "memory.img"))

	const concurrency = 4
	errs := make(chan error, concurrency)
	var wg sync.WaitGroup
	for i := range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("concurrent-%d.bin", i)
			data := bytes.Repeat([]byte{byte(i + 1)}, 8*1024*1024)
			if err := os.WriteFile(filepath.Join(source, name), data, 0o600); err != nil {
				errs <- err
				return
			}
			_, err := node.UploadSnapshot(context.Background(), &objectstorev1.UploadSnapshotRequest{
				SnapshotUri: fmt.Sprintf("azblob://%s/concurrent/%d", containerName, i),
				LocalPath:   source,
				Files:       []string{name},
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent UploadSnapshot: %v", err)
		}
	}
	sameUpload := &objectstorev1.UploadSnapshotRequest{
		SnapshotUri: "azblob://" + containerName + "/concurrent/same",
		LocalPath:   source,
		Files:       []string{"memory.img"},
	}
	errs = make(chan error, 2)
	for range 2 {
		go func() {
			_, err := node.UploadSnapshot(context.Background(), sameUpload)
			errs <- err
		}()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent same-URI UploadSnapshot: %v", err)
		}
	}

	copyReq := &objectstorev1.CopySnapshotRequest{
		SrcUri: snapshotURI,
		DstUri: "azblob://" + containerName + "/copied",
	}
	if _, err := control.CopySnapshot(context.Background(), copyReq); err != nil {
		t.Fatalf("CopySnapshot: %v", err)
	}
	if _, err := control.CopySnapshot(context.Background(), copyReq); err != nil {
		t.Fatalf("idempotent CopySnapshot: %v", err)
	}
	copiedDest := filepath.Join(root, "copied")
	if err := os.Mkdir(copiedDest, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := node.FetchSnapshot(context.Background(), &objectstorev1.FetchSnapshotRequest{
		SnapshotUri: copyReq.DstUri,
		WritePath:   copiedDest,
		Files:       []string{"memory.img"},
	}); err != nil {
		t.Fatalf("FetchSnapshot(copied): %v", err)
	}
	assertFileHash(t, largePath, filepath.Join(copiedDest, "memory.img"))

	crossReq := &objectstorev1.CopySnapshotRequest{
		SrcUri: snapshotURI,
		DstUri: "azblob://" + tagsContainer + "/stable",
	}
	if _, err := control.CopySnapshot(context.Background(), crossReq); err != nil {
		t.Fatalf("CopySnapshot across containers: %v", err)
	}
	if names, err := azure.List(context.Background(), "integration/stable/"); err != nil || len(names) != 0 {
		t.Fatalf("cross-container copy wrote %v (err %v) to the source container", names, err)
	}
	crossDest := filepath.Join(root, "cross")
	if err := os.Mkdir(crossDest, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := node.FetchSnapshot(context.Background(), &objectstorev1.FetchSnapshotRequest{
		SnapshotUri: crossReq.DstUri,
		WritePath:   crossDest,
		Files:       []string{"memory.img"},
	}); err != nil {
		t.Fatalf("FetchSnapshot(cross-container copy): %v", err)
	}
	assertFileHash(t, largePath, filepath.Join(crossDest, "memory.img"))
	if _, err := control.CleanupSnapshot(context.Background(), &objectstorev1.CleanupSnapshotRequest{SnapshotUri: crossReq.DstUri}); err != nil {
		t.Fatalf("CleanupSnapshot(cross-container copy): %v", err)
	}
	if names, err := tags.List(context.Background(), "integration/stable/"); err != nil || len(names) != 0 {
		t.Fatalf("cleaned cross-container copy still has %v (err %v)", names, err)
	}
	if _, err := control.CopySnapshot(context.Background(), &objectstorev1.CopySnapshotRequest{
		SrcUri: "azblob://" + containerName + "/missing",
		DstUri: "azblob://" + containerName + "/copied",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("empty CopySnapshot code = %v, want FailedPrecondition", status.Code(err))
	}

	if _, err := node.FetchSnapshot(context.Background(), &objectstorev1.FetchSnapshotRequest{
		SnapshotUri: "azblob://" + containerName + "/missing",
		WritePath:   dest,
		Files:       []string{"missing"},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing FetchSnapshot code = %v, want NotFound", status.Code(err))
	}

	corruptData := []byte("corrupt")
	sum := md5.Sum(corruptData)
	blockID := base64.StdEncoding.EncodeToString([]byte("corrupt-block"))
	corruptName := "integration/corrupt/file"
	if err := azure.StageBlock(context.Background(), corruptName, blockID, bytes.NewReader(corruptData), sum[:]); err != nil {
		t.Fatal(err)
	}
	wrongDigest := hex.EncodeToString(make([]byte, sha256.Size))
	if err := azure.CommitBlockList(context.Background(), corruptName, []string{blockID}, map[string]*string{integrityMetadata: &wrongDigest}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.FetchSnapshot(context.Background(), &objectstorev1.FetchSnapshotRequest{
		SnapshotUri: "azblob://" + containerName + "/corrupt",
		WritePath:   dest,
		Files:       []string{"file"},
	}); status.Code(err) != codes.DataLoss {
		t.Fatalf("corrupt FetchSnapshot code = %v, want DataLoss: %v", status.Code(err), err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := node.FetchSnapshot(canceled, fetch); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled FetchSnapshot code = %v, want Canceled", status.Code(err))
	}

	cleanup := &objectstorev1.CleanupSnapshotRequest{SnapshotUri: snapshotURI}
	if _, err := control.CleanupSnapshot(context.Background(), cleanup); err != nil {
		t.Fatalf("CleanupSnapshot: %v", err)
	}
	if _, err := control.CleanupSnapshot(context.Background(), cleanup); err != nil {
		t.Fatalf("idempotent CleanupSnapshot: %v", err)
	}
	names, err := azure.List(context.Background(), "integration/large/")
	if err != nil {
		t.Fatalf("listing cleaned snapshot: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("cleaned snapshot still has blobs: %v", names)
	}

	// A removed container holds no snapshots: cleanup succeeds, but reads
	// still report the misconfiguration.
	if _, err := serviceClient.NewContainerClient(tagsContainer).Delete(context.Background(), nil); err != nil {
		t.Fatalf("deleting Azurite container %s: %v", tagsContainer, err)
	}
	goneURI := "azblob://" + tagsContainer + "/gone"
	if _, err := control.CleanupSnapshot(context.Background(), &objectstorev1.CleanupSnapshotRequest{SnapshotUri: goneURI}); err != nil {
		t.Fatalf("CleanupSnapshot(missing container): %v", err)
	}
	if _, err := node.FetchSnapshot(context.Background(), &objectstorev1.FetchSnapshotRequest{
		SnapshotUri: goneURI,
		WritePath:   dest,
		Files:       []string{"file"},
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing container FetchSnapshot code = %v, want FailedPrecondition: %v", status.Code(err), err)
	}
}

// TestAzuriteKeepsBlobAndFileNames checks names the real adapter must not
// rewrite: a listed copy source suffix holding "..", a file name at the file
// system's 255-byte limit, and a file in the fs/ subdirectory gVisor
// checkpoints durable directories into.
func TestAzuriteKeepsBlobAndFileNames(t *testing.T) {
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
	containerName := fmt.Sprintf("names-%d", time.Now().UnixNano())
	containerClient := serviceClient.NewContainerClient(containerName)
	if _, err := containerClient.Create(context.Background(), nil); err != nil {
		t.Fatalf("creating Azurite container %s: %v", containerName, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = containerClient.Delete(ctx, nil)
	})
	azure, err := blobstore.NewAzureWithSharedKey(endpoint, containerName, account, key)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	node, control := serveIntegrationPlugin(t, map[string]blobstore.Store{containerName: azure}, root)
	ctx := context.Background()

	if err := azureLimits.upload(ctx, azure, "integration/src/../victim/file", bytesSource([]byte("payload"), int64(len("payload"))), ""); err != nil {
		t.Fatal(err)
	}
	if names, err := azure.List(ctx, "integration/src/"); err != nil || len(names) != 1 || names[0] != "integration/src/../victim/file" {
		t.Fatalf("Azurite listed %v (err %v), want the raw source name", names, err)
	}
	if _, err := control.CopySnapshot(ctx, &objectstorev1.CopySnapshotRequest{
		SrcUri: "azblob://" + containerName + "/src",
		DstUri: "azblob://" + containerName + "/dst",
	}); err != nil {
		t.Fatalf("CopySnapshot: %v", err)
	}
	if names, err := azure.List(ctx, "integration/dst/"); err != nil || len(names) != 1 || names[0] != "integration/dst/../victim/file" {
		t.Fatalf("destination holds %v (err %v), want the source suffix kept", names, err)
	}
	if names, err := azure.List(ctx, "integration/victim/"); err != nil || len(names) != 0 {
		t.Fatalf("CopySnapshot wrote %v (err %v) outside the destination prefix", names, err)
	}

	source := filepath.Join(root, "source")
	dest := filepath.Join(root, "dest")
	for _, dir := range []string{source, dest} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	name := strings.Repeat("n", 255)
	if err := os.WriteFile(filepath.Join(source, name), []byte(name), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshotURI := "azblob://" + containerName + "/long"
	if _, err := node.UploadSnapshot(ctx, &objectstorev1.UploadSnapshotRequest{
		SnapshotUri: snapshotURI, LocalPath: source, Files: []string{name},
	}); err != nil {
		t.Fatalf("UploadSnapshot(255-byte name): %v", err)
	}
	if _, err := node.FetchSnapshot(ctx, &objectstorev1.FetchSnapshotRequest{
		SnapshotUri: snapshotURI, WritePath: dest, Files: []string{name},
	}); err != nil {
		t.Fatalf("FetchSnapshot(255-byte name): %v", err)
	}
	assertFileHash(t, filepath.Join(source, name), filepath.Join(dest, name))

	nested := "fs/fscheckpoint.pb"
	if err := os.Mkdir(filepath.Join(source, "fs"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, nested), []byte("fs manifest"), 0o600); err != nil {
		t.Fatal(err)
	}
	nestedURI := "azblob://" + containerName + "/nested"
	if _, err := node.UploadSnapshot(ctx, &objectstorev1.UploadSnapshotRequest{
		SnapshotUri: nestedURI, LocalPath: source, Files: []string{nested},
	}); err != nil {
		t.Fatalf("UploadSnapshot(%s): %v", nested, err)
	}
	if names, err := azure.List(ctx, "integration/nested/"); err != nil || len(names) != 1 || names[0] != "integration/nested/"+nested {
		t.Fatalf("Azurite listed %v (err %v), want integration/nested/%s", names, err, nested)
	}
	if _, err := node.FetchSnapshot(ctx, &objectstorev1.FetchSnapshotRequest{
		SnapshotUri: nestedURI, WritePath: dest, Files: []string{nested},
	}); err != nil {
		t.Fatalf("FetchSnapshot(%s): %v", nested, err)
	}
	assertFileHash(t, filepath.Join(source, nested), filepath.Join(dest, nested))
}

func serveIntegrationPlugin(t *testing.T, stores map[string]blobstore.Store, root string) (objectstorev1.NodeProviderClient, objectstorev1.ControlProviderClient) {
	t.Helper()
	server, err := New(stores, "integration", root)
	if err != nil {
		t.Fatal(err)
	}
	socketDir, err := os.MkdirTemp("", "azure-snapshot")
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
	objectstorev1.RegisterNodeProviderServer(grpcServer, server)
	objectstorev1.RegisterControlProviderServer(grpcServer, server)
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
	return objectstorev1.NewNodeProviderClient(conn), objectstorev1.NewControlProviderClient(conn)
}

func assertFileHash(t *testing.T, paths ...string) {
	t.Helper()
	var want []byte
	for _, name := range paths {
		file, err := os.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.New()
		if _, err := io.Copy(hash, file); err != nil {
			file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		got := hash.Sum(nil)
		if want == nil {
			want = got
		} else if !bytes.Equal(got, want) {
			t.Fatalf("%s SHA-256 = %x, want %x", name, got, want)
		}
	}
}

// TestAzuriteStreamsLargeBlocks uploads and copies blocks larger than
// bufferBudget, which the SDK streams from the file and from ranged downloads
// of the source, and checks that Azurite commits them whole and that a block
// staged with the wrong MD5 is a retryable failure.
func TestAzuriteStreamsLargeBlocks(t *testing.T) {
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
	containerName := fmt.Sprintf("stream-%d", time.Now().UnixNano())
	containerClient := serviceClient.NewContainerClient(containerName)
	if _, err := containerClient.Create(context.Background(), nil); err != nil {
		t.Fatalf("creating Azurite container %s: %v", containerName, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = containerClient.Delete(ctx, nil)
	})
	azure, err := blobstore.NewAzureWithSharedKey(endpoint, containerName, account, key)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	server, err := New(map[string]blobstore.Store{containerName: azure}, "integration", root)
	if err != nil {
		t.Fatal(err)
	}
	server.limits = streamedLimits
	ctx := context.Background()
	source := filepath.Join(root, "source")
	for _, dir := range []string{source, filepath.Join(root, "dest"), filepath.Join(root, "copied")} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(source, "memory.img"), patterned(streamedSize), 0o600); err != nil {
		t.Fatal(err)
	}
	committedBlocks := func(name string) []int64 {
		t.Helper()
		list, err := containerClient.NewBlockBlobClient(name).GetBlockList(ctx, blockblob.BlockListTypeCommitted, nil)
		if err != nil {
			t.Fatalf("GetBlockList(%s): %v", name, err)
		}
		var sizes []int64
		for _, block := range list.CommittedBlocks {
			sizes = append(sizes, *block.Size)
		}
		return sizes
	}

	uri := "azblob://" + containerName + "/streamed"
	if _, err := server.UploadSnapshot(ctx, &objectstorev1.UploadSnapshotRequest{SnapshotUri: uri, LocalPath: source, Files: []string{"memory.img"}}); err != nil {
		t.Fatalf("UploadSnapshot: %v", err)
	}
	if got := committedBlocks("integration/streamed/memory.img"); !slices.Equal(got, []int64{streamedBlock, streamedBlock}) {
		t.Fatalf("uploaded blocks %v, want two of %d bytes", got, streamedBlock)
	}
	copied := "azblob://" + containerName + "/copied"
	if _, err := server.CopySnapshot(ctx, &objectstorev1.CopySnapshotRequest{SrcUri: uri, DstUri: copied}); err != nil {
		t.Fatalf("CopySnapshot: %v", err)
	}
	if got := committedBlocks("integration/copied/memory.img"); !slices.Equal(got, []int64{streamedBlock, streamedBlock}) {
		t.Fatalf("copied blocks %v, want two of %d bytes", got, streamedBlock)
	}
	for snapshot, dir := range map[string]string{uri: "dest", copied: "copied"} {
		if _, err := server.FetchSnapshot(ctx, &objectstorev1.FetchSnapshotRequest{SnapshotUri: snapshot, WritePath: filepath.Join(root, dir), Files: []string{"memory.img"}}); err != nil {
			t.Fatalf("FetchSnapshot(%s): %v", snapshot, err)
		}
		assertFileHash(t, filepath.Join(source, "memory.img"), filepath.Join(root, dir, "memory.img"))
	}

	wrong := md5.Sum([]byte("other"))
	err = azure.StageBlock(ctx, "integration/mismatch/file", base64.StdEncoding.EncodeToString([]byte("block")), strings.NewReader("block"), wrong[:])
	if !errors.Is(err, blobstore.ErrUnavailable) {
		t.Fatalf("StageBlock with a mismatched MD5 = %v, want ErrUnavailable", err)
	}
}
