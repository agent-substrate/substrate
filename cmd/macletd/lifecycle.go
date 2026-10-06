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

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/agent-substrate/substrate/pkg/proto/hostruntimepb"
	objectstoresnapshotv1 "github.com/agent-substrate/substrate/pkg/proto/objectstoresnapshotpb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	localReceiptName  = "macletd-local-snapshot.json"
	sourceReceiptName = "macletd-snapshot-source.json"
	manifestName      = "manifest.json"
)

var snapshotFiles = []string{"actor.json", "disk.img", "nvram.bin"}

type snapshotFile struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}
type snapshotManifest struct {
	SchemaVersion int                     `json:"schemaVersion"`
	ContentScope  string                  `json:"contentScope"`
	Image         string                  `json:"image"`
	Files         map[string]snapshotFile `json:"files"`
}
type receipt struct {
	Value string `json:"value"`
}

func writeJSONAtomic(path string, value any) error {
	dir := filepath.Dir(path)
	t, err := os.CreateTemp(dir, ".receipt-*")
	if err != nil {
		return err
	}
	name := t.Name()
	defer os.Remove(name)
	if err := t.Chmod(0o600); err != nil {
		_ = t.Close()
		return err
	}
	if err := json.NewEncoder(t).Encode(value); err != nil {
		_ = t.Close()
		return err
	}
	if err := t.Sync(); err != nil {
		_ = t.Close()
		return err
	}
	if err := t.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	return errors.Join(syncErr, d.Close())
}

func readReceipt(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	var r receipt
	d := json.NewDecoder(io.LimitReader(f, 4097))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return "", fmt.Errorf("invalid receipt: %w", err)
	}
	if r.Value == "" {
		return "", errors.New("invalid receipt: empty value")
	}
	return r.Value, nil
}
func readLocalReceipt(bundle string) (string, error) {
	return readReceipt(filepath.Join(bundle, localReceiptName))
}
func (s *server) receiptPath(uid string) string {
	return filepath.Join(s.stateDir, ".receipts", uid+".json")
}
func (s *server) readExternalReceipt(uid string) (string, error) {
	return readReceipt(s.receiptPath(uid))
}

func (s *server) stopRetain(ctx context.Context, uid, bundle string) error {
	if err := s.runner.Run(ctx, io.Discard, s.maclet, "stop", bundle); err != nil {
		return fmt.Errorf("stop maclet: %w", err)
	}
	s.mu.Lock()
	child, proxy := s.children[uid], s.proxies[uid]
	s.mu.Unlock()
	if child != nil {
		select {
		case <-child.done:
		case <-ctx.Done():
			return ctx.Err()
		}
		s.mu.Lock()
		delete(s.children, uid)
		s.mu.Unlock()
	}
	var errs []error
	if proxy != nil {
		if err := proxy.server.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
		s.mu.Lock()
		delete(s.proxies, uid)
		s.mu.Unlock()
	}
	_ = os.Remove(filepath.Join(bundle, proxyMetadataName))
	if err := s.volumes.Unstage(ctx, uid); err != nil {
		errs = append(errs, fmt.Errorf("unstage durable volumes: %w", err))
	} else if err := writeVMVolumeConfig(bundle, nil); err != nil {
		errs = append(errs, fmt.Errorf("remove durable volume configuration: %w", err))
	}
	return errors.Join(errs...)
}

func (s *server) Pause(ctx context.Context, req *hostruntimepb.PauseRequest) (*hostruntimepb.PauseResponse, error) {
	if req == nil || validateUID(req.GetActorUid()) != nil || validateUID(req.GetLocalSnapshotName()) != nil {
		return nil, status.Error(codes.InvalidArgument, "valid actor_uid and local_snapshot_name are required")
	}
	lock := s.actorLock(req.GetActorUid())
	lock.Lock()
	defer lock.Unlock()
	bundle := s.bundle(req.GetActorUid())
	exists, err := safeDirectory(bundle)
	if err != nil || !exists {
		return nil, status.Error(codes.FailedPrecondition, "actor bundle does not exist")
	}
	st, statusErr := s.readStatus(ctx, bundle)
	if old, err := readLocalReceipt(bundle); err == nil {
		if old == req.GetLocalSnapshotName() {
			// Continue through stopRetain even when already stopped: a previous
			// attempt may have stopped the guest but failed to unstage storage.
		} else if statusErr != nil || st.Phase == "stopped" {
			return nil, status.Error(codes.FailedPrecondition, "Actor already has a different local snapshot name")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	// Persist the pause intent before stopping. A retry with the same name is
	// idempotent across a daemon crash; after a successful local resume, a
	// running VM may advance the receipt to the next pause's newly minted name.
	if err := writeJSONAtomic(filepath.Join(bundle, localReceiptName), receipt{req.GetLocalSnapshotName()}); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := s.stopRetain(ctx, req.GetActorUid(), bundle); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &hostruntimepb.PauseResponse{}, nil
}

func fileDigest(path string) (snapshotFile, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return snapshotFile{}, fmt.Errorf("unsafe snapshot file %q", filepath.Base(path))
	}
	f, err := os.Open(path)
	if err != nil {
		return snapshotFile{}, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return snapshotFile{}, err
	}
	return snapshotFile{Size: info.Size(), SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

func (s *server) Checkpoint(ctx context.Context, req *hostruntimepb.CheckpointRequest) (*hostruntimepb.CheckpointResponse, error) {
	if req == nil || validateUID(req.GetActorUid()) != nil || strings.TrimSpace(req.GetExternalSnapshotUri()) == "" {
		return nil, status.Error(codes.InvalidArgument, "valid actor_uid and external_snapshot_uri are required")
	}
	uid, uri := req.GetActorUid(), req.GetExternalSnapshotUri()
	lock := s.actorLock(uid)
	lock.Lock()
	defer lock.Unlock()
	if old, err := s.readExternalReceipt(uid); err == nil {
		if old != uri {
			return nil, status.Error(codes.FailedPrecondition, "Actor already has a different external snapshot URI")
		}
		if exists, _ := safeDirectory(s.bundle(uid)); !exists {
			return &hostruntimepb.CheckpointResponse{}, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if s.snapshots == nil {
		return nil, status.Error(codes.FailedPrecondition, "snapshot provider is not configured")
	}
	bundle := s.bundle(uid)
	exists, err := safeDirectory(bundle)
	if err != nil || !exists {
		return nil, status.Error(codes.FailedPrecondition, "actor bundle does not exist")
	}
	if err := os.MkdirAll(filepath.Join(s.stateDir, ".receipts"), 0o700); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := writeJSONAtomic(s.receiptPath(uid), receipt{uri}); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := s.stopRetain(ctx, uid, bundle); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	stage, err := os.MkdirTemp(s.stateDir, ".snapshot-*")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer os.RemoveAll(stage)
	if err := os.Chmod(stage, 0o700); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	shot := filepath.Join(stage, "bundle")
	if err := s.runner.Run(ctx, io.Discard, s.maclet, "snapshot", bundle, shot); err != nil {
		return nil, status.Errorf(codes.Internal, "capture snapshot: %v", err)
	}
	m := snapshotManifest{SchemaVersion: 1, ContentScope: "disk", Image: s.image, Files: map[string]snapshotFile{}}
	for _, name := range snapshotFiles {
		f, err := fileDigest(filepath.Join(shot, name))
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		m.Files[name] = f
	}
	if err := writeJSONAtomic(filepath.Join(shot, manifestName), m); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if _, err := s.snapshots.UploadSnapshot(ctx, &objectstoresnapshotv1.UploadSnapshotRequest{SnapshotUri: uri, LocalPath: shot, Files: snapshotFiles}); err != nil {
		return nil, status.Errorf(codes.Unavailable, "upload snapshot data: %v", err)
	}
	if _, err := s.snapshots.UploadSnapshot(ctx, &objectstoresnapshotv1.UploadSnapshotRequest{SnapshotUri: uri, LocalPath: shot, Files: []string{manifestName}}); err != nil {
		return nil, status.Errorf(codes.Unavailable, "commit snapshot manifest: %v", err)
	}
	if err := os.RemoveAll(bundle); err != nil {
		return nil, status.Errorf(codes.Internal, "remove disposable bundle: %v", err)
	}
	return &hostruntimepb.CheckpointResponse{}, nil
}

func (s *server) restoreExternal(ctx context.Context, uid, uri, destination string, cpuMilli, memoryBytes int64) error {
	if s.snapshots == nil {
		return errors.New("snapshot provider is not configured")
	}
	stage, err := os.MkdirTemp(s.stateDir, ".restore-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	if err := os.Chmod(stage, 0o700); err != nil {
		return err
	}
	if _, err := s.snapshots.FetchSnapshot(ctx, &objectstoresnapshotv1.FetchSnapshotRequest{SnapshotUri: uri, WritePath: stage, Files: []string{manifestName}}); err != nil {
		return err
	}
	b, err := os.ReadFile(filepath.Join(stage, manifestName))
	if err != nil || len(b) > 64<<10 {
		return errors.New("invalid snapshot manifest")
	}
	var m snapshotManifest
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil || m.SchemaVersion != 1 || m.ContentScope != "disk" || m.Image != s.image || len(m.Files) != len(snapshotFiles) {
		return errors.New("snapshot manifest is incompatible")
	}
	if _, err := s.snapshots.FetchSnapshot(ctx, &objectstoresnapshotv1.FetchSnapshotRequest{SnapshotUri: uri, WritePath: stage, Files: snapshotFiles}); err != nil {
		return err
	}
	for _, name := range snapshotFiles {
		got, err := fileDigest(filepath.Join(stage, name))
		if err != nil || got != m.Files[name] {
			return fmt.Errorf("snapshot file %s failed size/checksum validation", name)
		}
	}
	args := []string{"restore", stage, destination, uid}
	if cpuMilli > 0 {
		args = append(args, fmt.Sprint((cpuMilli+999)/1000), fmt.Sprint(memoryBytes))
	}
	if err := s.runner.Run(ctx, io.Discard, s.maclet, args...); err != nil {
		return err
	}
	if err := writeJSONAtomic(filepath.Join(destination, sourceReceiptName), receipt{uri}); err != nil {
		_ = os.RemoveAll(destination)
		return err
	}
	// A receipt outside the bundle records the previous completed checkpoint.
	// Once a new execution exists, its next suspend intentionally writes a new
	// URI, so the old completion receipt must not constrain it.
	if err := os.Remove(s.receiptPath(uid)); err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = os.RemoveAll(destination)
		return err
	}
	return nil
}

func (s *server) Discard(ctx context.Context, req *hostruntimepb.DiscardRequest) (*hostruntimepb.DiscardResponse, error) {
	if req == nil || validateUID(req.GetActorUid()) != nil {
		return nil, status.Error(codes.InvalidArgument, "valid actor_uid is required")
	}
	uid := req.GetActorUid()
	lock := s.actorLock(uid)
	lock.Lock()
	defer lock.Unlock()
	bundle := s.bundle(uid)
	exists, err := safeDirectory(bundle)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if exists {
		if err := s.stopRetain(ctx, uid, bundle); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if err := os.RemoveAll(bundle); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else if err := s.volumes.Unstage(ctx, uid); err != nil {
		return nil, status.Errorf(codes.Internal, "unstage durable volumes: %v", err)
	}
	if err := os.Remove(s.receiptPath(uid)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &hostruntimepb.DiscardResponse{}, nil
}
