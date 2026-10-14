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

package provider

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/agent-substrate/substrate/internal/plugins/azure-blob/internal/blobstore"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testAccount = "https://acct.blob.core.windows.net"
	untouched   = "the caller's original bytes"
	// testCapacity is the fixtures' staging capacity.
	testCapacity = 1 << 20
)

// countingStore counts downloads, so a test can tell that a request was
// refused before anything was read, can fail them with downloadErr, and can
// leave their size unreported.
type countingStore struct {
	*fakeStore
	downloads   atomic.Int64
	downloadErr error
	unknownSize bool
}

func (c *countingStore) Download(ctx context.Context, name string) (blobstore.Download, error) {
	c.downloads.Add(1)
	if c.downloadErr != nil {
		return blobstore.Download{}, c.downloadErr
	}
	d, err := c.fakeStore.Download(ctx, name)
	if c.unknownSize {
		d.Size = -1
	}
	return d, err
}

// assetFixture is an AssetServer on the containers "assets" and "snapshots",
// next to the snapshot Server that writes "snapshots" under the prefix
// "substrate", with a write root, a separate staging directory and an
// existing caller file below the root.
type assetFixture struct {
	server    *AssetServer
	snapshots *Server
	stores    map[string]*countingStore
	root      string
	staging   string
	dst       string
}

func newAssetFixture(t *testing.T) *assetFixture {
	t.Helper()
	f := &assetFixture{
		stores:  map[string]*countingStore{"assets": {fakeStore: newFakeStore()}, "snapshots": {fakeStore: newFakeStore()}},
		root:    t.TempDir(),
		staging: t.TempDir(),
	}
	stores := map[string]blobstore.Store{}
	for name, store := range f.stores {
		stores[name] = store
	}
	var err error
	if f.server, err = NewAssetServer(testAccount, stores, f.root, f.staging, testCapacity); err != nil {
		t.Fatal(err)
	}
	if f.snapshots, err = New(map[string]blobstore.Store{"snapshots": f.stores["snapshots"]}, "substrate", f.root); err != nil {
		t.Fatal(err)
	}
	f.dst = filepath.Join(f.root, "static-files", "asset-download-1")
	if err := os.MkdirAll(filepath.Dir(f.dst), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.dst, []byte(untouched), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

// put stores content as container/blob and returns its sha256.
func (f *assetFixture) put(container, blob, content string) string {
	f.stores[container].blobs[blob] = fakeBlob{data: []byte(content)}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(content)))
}

func (f *assetFixture) fetch(t *testing.T, uri, sha string, maxBytes int64) error {
	t.Helper()
	_, err := f.server.FetchAsset(t.Context(), &objectstorev1.FetchAssetRequest{
		AssetUri: uri, Sha256: sha, WritePath: f.dst, MaxBytes: maxBytes,
	})
	return err
}

func (f *assetFixture) downloads() int64 {
	var n int64
	for _, s := range f.stores {
		n += s.downloads.Load()
	}
	return n
}

func (f *assetFixture) assertDst(t *testing.T, want string) {
	t.Helper()
	got, err := os.ReadFile(f.dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Errorf("write path holds %q, want %q", got, want)
	}
}

func (f *assetFixture) assertStagingEmpty(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(f.staging)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("staging directory holds %d entries after the call, want none", len(entries))
	}
}

func TestFetchAssetServesAccountBlobs(t *testing.T) {
	f := newAssetFixture(t)
	for _, tc := range []struct{ container, blob, uri string }{
		{"assets", "vmlinux", testAccount + "/assets/vmlinux"},
		{"assets", "kata-assets/rootfs.img", testAccount + "/assets/kata-assets/rootfs.img"},
		// Host names are case-insensitive.
		{"assets", "virtiofsd", "https://ACCT.blob.core.windows.net/assets/virtiofsd"},
		// A blob outside any snapshot location in a snapshot container.
		{"snapshots", "kata-assets/cloud-hypervisor", testAccount + "/snapshots/kata-assets/cloud-hypervisor"},
		// Names that share segments with the snapshot layout.
		{"assets", "substrate/atespaces/readme", testAccount + "/assets/substrate/atespaces/readme"},
		{"assets", "substrate/atespaces/team-a/actors/uid1", testAccount + "/assets/substrate/atespaces/team-a/actors/uid1"},
	} {
		content := "content of " + tc.blob
		sum := f.put(tc.container, tc.blob, content)
		if err := f.fetch(t, tc.uri, sum, int64(len(content))); err != nil {
			t.Errorf("FetchAsset(%s) = %v, want success", tc.uri, err)
			continue
		}
		f.assertDst(t, content)
	}
	f.assertStagingEmpty(t)
}

// TestFetchAssetRefusesSnapshots writes a snapshot and a tag through the
// snapshot Server, so the blob names are the ones the plugin itself stores,
// and asks for each blob by its native URL with its correct sha256: every
// request must be refused before anything is downloaded, whichever container
// it names.
func TestFetchAssetRefusesSnapshots(t *testing.T) {
	f := newAssetFixture(t)
	src := filepath.Join(f.root, "src")
	if err := os.Mkdir(src, 0o700); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{"manifest.json": "{}", "pages.img": "guest memory", "checkpoint.img": "checkpoint"}
	var names []string
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(src, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	const (
		snapshotURI = "azblob://snapshots/hq-e2e/ate-demo/atespaces/ate-demo/actors/4805103b-9060-44bc-b54a-a220f9170186/snapshots/85594b93-62ef-4664-9346-2960302b58b5"
		tagURI      = "azblob://snapshots/hq-e2e/ate-demo/atespaces/ate-golden/tags/d33a8f2f-c6ce-45b7-9659-69d1fee12fa5"
	)
	if _, err := f.snapshots.UploadSnapshot(t.Context(), &objectstorev1.UploadSnapshotRequest{SnapshotUri: snapshotURI, LocalPath: src, Files: names}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.snapshots.CopySnapshot(t.Context(), &objectstorev1.CopySnapshotRequest{SrcUri: snapshotURI, DstUri: tagURI}); err != nil {
		t.Fatal(err)
	}
	stored := f.stores["snapshots"].blobs
	if len(stored) != 2*len(files) {
		t.Fatalf("snapshot store holds %d blobs, want %d", len(stored), 2*len(files))
	}
	// CopySnapshot read the source; count only FetchAsset's downloads.
	f.stores["snapshots"].downloads.Store(0)

	for blob, b := range stored {
		sum := fmt.Sprintf("%x", sha256.Sum256(b.data))
		// The same blob name in an asset container and in a container the
		// plugin does not serve at all is refused the same way.
		for _, container := range []string{"snapshots", "assets", "other"} {
			uri := testAccount + "/" + container + "/" + blob
			err := f.fetch(t, uri, sum, 1<<20)
			if status.Code(err) != codes.PermissionDenied {
				t.Errorf("FetchAsset(%s) = %v, want %s", uri, err, codes.PermissionDenied)
			}
		}
	}
	// The bare snapshot and tag prefixes, in case a blob has that name.
	for _, prefix := range []string{
		"substrate/hq-e2e/ate-demo/atespaces/ate-demo/actors/4805103b-9060-44bc-b54a-a220f9170186/snapshots/85594b93-62ef-4664-9346-2960302b58b5",
		"substrate/hq-e2e/ate-demo/atespaces/ate-golden/tags/d33a8f2f-c6ce-45b7-9659-69d1fee12fa5",
	} {
		uri := testAccount + "/snapshots/" + prefix
		if err := f.fetch(t, uri, strings.Repeat("0", 64), 1<<20); status.Code(err) != codes.PermissionDenied {
			t.Errorf("FetchAsset(%s) = %v, want %s", uri, err, codes.PermissionDenied)
		}
	}
	if n := f.downloads(); n != 0 {
		t.Errorf("refused requests downloaded %d blobs, want 0", n)
	}
	f.assertDst(t, untouched)
	f.assertStagingEmpty(t)
}

func TestFetchAssetRejectsURIs(t *testing.T) {
	f := newAssetFixture(t)
	sum := f.put("assets", "vmlinux", "kernel")
	for _, uri := range []string{
		"azblob://assets/vmlinux",
		"gs://assets/vmlinux",
		"http://acct.blob.core.windows.net/assets/vmlinux",
		"https://other.blob.core.windows.net/assets/vmlinux",
		"https://acct.blob.core.windows.net.evil.example/assets/vmlinux",
		"https://acct.blob.core.windows.net:8443/assets/vmlinux",
		"https://acct.blob.core.windows.net/assets/vmlinux?sv=2024&sig=secret",
		"https://user:secret@acct.blob.core.windows.net/assets/vmlinux",
		"https://acct.blob.core.windows.net/assets/vmlinux#x",
		"https://acct.blob.core.windows.net",
		"https://acct.blob.core.windows.net/",
		"https://acct.blob.core.windows.net/assets",
		"https://acct.blob.core.windows.net/assets/",
		"https://acct.blob.core.windows.net//vmlinux",
		"https://acct.blob.core.windows.net/other/vmlinux",
		"https://acct.blob.core.windows.net/assets/a//vmlinux",
		"https://acct.blob.core.windows.net/assets/a/../vmlinux",
		"https://acct.blob.core.windows.net/assets/a%2F..%2Fvmlinux",
		`https://acct.blob.core.windows.net/assets/a\vmlinux`,
		"https://acct.blob.core.windows.net/assets/vmlinux.",
		"https://acct.blob.core.windows.net/assets/" + strings.Repeat("x/", 1024) + "vmlinux",
	} {
		err := f.fetch(t, uri, sum, 1<<20)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("FetchAsset(%.80s) = %v, want %s", uri, err, codes.InvalidArgument)
		}
		if err != nil && strings.Contains(err.Error(), "secret") {
			t.Errorf("FetchAsset(%s) error %q quotes credentials from the URI", uri, err)
		}
	}
	if n := f.downloads(); n != 0 {
		t.Errorf("rejected requests downloaded %d blobs, want 0", n)
	}
	f.assertDst(t, untouched)
}

func TestFetchAssetVerifiesBeforeWriting(t *testing.T) {
	f := newAssetFixture(t)
	const kernel = "kernel"
	sum := f.put("assets", "vmlinux", kernel)
	uri := testAccount + "/assets/vmlinux"
	for _, tc := range []struct {
		name     string
		sha      string
		maxBytes int64
	}{
		{"sha256 mismatch", fmt.Sprintf("%x", sha256.Sum256([]byte("other"))), 1 << 20},
		{"one byte over max_bytes", sum, int64(len(kernel) - 1)},
	} {
		if err := f.fetch(t, uri, tc.sha, tc.maxBytes); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("%s: FetchAsset = %v, want %s", tc.name, err, codes.FailedPrecondition)
		}
	}
	f.assertDst(t, untouched)
	f.assertStagingEmpty(t)
	if err := f.fetch(t, uri, sum, int64(len(kernel))); err != nil {
		t.Fatalf("FetchAsset at exactly max_bytes = %v, want success", err)
	}
	f.assertDst(t, kernel)
}

// TestFetchAssetStagingCapacity covers the staging bound: a blob whose
// reported size exceeds the capacity is refused before it is read, and one
// whose size the service did not report is read only up to the capacity.
func TestFetchAssetStagingCapacity(t *testing.T) {
	big := strings.Repeat("x", testCapacity+1)
	for _, tc := range []struct {
		name        string
		unknownSize bool
	}{
		{name: "reported size", unknownSize: false},
		{name: "unreported size", unknownSize: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAssetFixture(t)
			sum := f.put("assets", "rootfs.img", big)
			f.stores["assets"].unknownSize = tc.unknownSize
			err := f.fetch(t, testAccount+"/assets/rootfs.img", sum, 8<<30)
			if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "staging capacity") {
				t.Fatalf("FetchAsset of a blob larger than the staging capacity = %v, want %s naming the capacity", err, codes.FailedPrecondition)
			}
			f.assertDst(t, untouched)
			f.assertStagingEmpty(t)
		})
	}

	// A blob without a reported size within the capacity is served.
	f := newAssetFixture(t)
	sum := f.put("assets", "vmlinux", "kernel")
	f.stores["assets"].unknownSize = true
	if err := f.fetch(t, testAccount+"/assets/vmlinux", sum, 1<<20); err != nil {
		t.Fatalf("FetchAsset with no reported size = %v, want success", err)
	}
	f.assertDst(t, "kernel")
}

func TestFetchAssetStorageErrors(t *testing.T) {
	for _, tc := range []struct {
		name        string
		downloadErr error
		readErr     error
		blob        string
		want        codes.Code
	}{
		{name: "absent blob", blob: "missing", want: codes.NotFound},
		{name: "absent container", downloadErr: fmt.Errorf("%w: gone", blobstore.ErrContainerNotFound), want: codes.NotFound},
		// PermissionDenied is the snapshot refusal; a storage authorization
		// failure must not look like one.
		{name: "storage denies the identity", downloadErr: fmt.Errorf("%w: Azure returned HTTP 403 AuthorizationPermissionMismatch", blobstore.ErrPermission), want: codes.Internal},
		{name: "unavailable on open", downloadErr: fmt.Errorf("%w: HTTP 503", blobstore.ErrUnavailable), want: codes.Unavailable},
		{name: "connection reset mid-body", readErr: fmt.Errorf("%w: connection reset", blobstore.ErrUnavailable), want: codes.Unavailable},
		{name: "canceled", downloadErr: context.Canceled, want: codes.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAssetFixture(t)
			sum := f.put("assets", "vmlinux", "kernel")
			store := f.stores["assets"]
			store.downloadErr = tc.downloadErr
			store.readErr = tc.readErr
			blob := "vmlinux"
			if tc.blob != "" {
				blob = tc.blob
			}
			err := f.fetch(t, testAccount+"/assets/"+blob, sum, 1<<20)
			if status.Code(err) != tc.want {
				t.Fatalf("FetchAsset = %v, want %s", err, tc.want)
			}
			f.assertDst(t, untouched)
			f.assertStagingEmpty(t)
		})
	}
}

// TestFetchAssetPathStyleAccount serves an account whose URL carries the
// account name in its path, as Azurite and other emulators do.
func TestFetchAssetPathStyleAccount(t *testing.T) {
	root, staging := t.TempDir(), t.TempDir()
	store := &countingStore{fakeStore: newFakeStore()}
	server, err := NewAssetServer("http://127.0.0.1:10000/devstoreaccount1/", map[string]blobstore.Store{"assets": store}, root, staging, testCapacity)
	if err != nil {
		t.Fatal(err)
	}
	store.blobs["vmlinux"] = fakeBlob{data: []byte("kernel")}
	dst := filepath.Join(root, "dst")
	if err := os.WriteFile(dst, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := fmt.Sprintf("%x", sha256.Sum256([]byte("kernel")))
	for uri, want := range map[string]codes.Code{
		"http://127.0.0.1:10000/devstoreaccount1/assets/vmlinux":  codes.OK,
		"http://127.0.0.1:10000/assets/vmlinux":                   codes.InvalidArgument,
		"http://127.0.0.1:10000/devstoreaccount2/assets/vmlinux":  codes.InvalidArgument,
		"http://127.0.0.1:10000/devstoreaccount1x/assets/vmlinux": codes.InvalidArgument,
	} {
		_, err := server.FetchAsset(t.Context(), &objectstorev1.FetchAssetRequest{AssetUri: uri, Sha256: sum, WritePath: dst, MaxBytes: 1 << 20})
		if status.Code(err) != want {
			t.Errorf("FetchAsset(%s) = %v, want %s", uri, err, want)
		}
	}
}

func TestNewAssetServerRejectsInvalidConfiguration(t *testing.T) {
	root := t.TempDir()
	stores := map[string]blobstore.Store{"assets": newFakeStore()}
	for _, tc := range []struct {
		name, account string
		stores        map[string]blobstore.Store
		root, staging string
		capacity      int64
	}{
		{"no containers", testAccount, nil, root, t.TempDir(), testCapacity},
		{"invalid container", testAccount, map[string]blobstore.Store{"Assets": newFakeStore()}, root, t.TempDir(), testCapacity},
		{"nil store", testAccount, map[string]blobstore.Store{"assets": nil}, root, t.TempDir(), testCapacity},
		{"account without host", "https://", stores, root, t.TempDir(), testCapacity},
		{"account with query", testAccount + "?sig=secret", stores, root, t.TempDir(), testCapacity},
		{"account with userinfo", "https://user:secret@" + strings.TrimPrefix(testAccount, "https://"), stores, root, t.TempDir(), testCapacity},
		{"staging is root", testAccount, stores, root, root, testCapacity},
		{"staging below root", testAccount, stores, root, filepath.Join(root, "staging"), testCapacity},
		{"relative root", testAccount, stores, "relative", t.TempDir(), testCapacity},
		{"zero staging capacity", testAccount, stores, root, t.TempDir(), 0},
	} {
		_, err := NewAssetServer(tc.account, tc.stores, tc.root, tc.staging, tc.capacity)
		if err == nil {
			t.Errorf("%s: NewAssetServer succeeded, want an error", tc.name)
		} else if strings.Contains(err.Error(), "secret") {
			t.Errorf("%s: NewAssetServer error %q repeats the account URL credential", tc.name, err)
		}
	}
}
