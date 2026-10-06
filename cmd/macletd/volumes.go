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
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/agent-substrate/substrate/internal/mountinfo"
	"github.com/agent-substrate/substrate/pkg/proto/hostruntimepb"
)

const (
	nfsCSIDriver         = "nfs.csi.k8s.io"
	vmVolumeConfigName   = "macletd-volumes.json"
	guestConfigDirectory = "macletd-guest-config"
)

var nfsServerPattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$`)

type stagedVolume struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	Tag       string `json:"tag"`
	HostPath  string `json:"hostPath"`
}

type guestVolume struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	Tag       string `json:"tag"`
}

type volumeStager interface {
	Stage(context.Context, string, []*hostruntimepb.DurableVolume) ([]stagedVolume, error)
	Unstage(context.Context, string) error
}

type mountOperations interface {
	Mounted(context.Context, string) (bool, error)
	MountNFS(context.Context, string, string) error
	Unmount(context.Context, string) error
}

type hostMountOperations struct{}

func (hostMountOperations) Mounted(_ context.Context, target string) (bool, error) {
	return mountinfo.Mounted(target, "nfs", "nfs4")
}

func (hostMountOperations) MountNFS(ctx context.Context, remote, target string) error {
	output, err := exec.CommandContext(ctx, "/sbin/mount_nfs", "-o", "nosuid,nodev", remote, target).CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount_nfs: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (hostMountOperations) Unmount(ctx context.Context, target string) error {
	output, err := exec.CommandContext(ctx, "/sbin/umount", target).CombinedOutput()
	if err != nil {
		return fmt.Errorf("umount: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

type nfsVolumeStager struct {
	root   string
	mounts mountOperations
}

type nfsStageReceipt struct {
	SchemaVersion int    `json:"schemaVersion"`
	VolumeID      string `json:"volumeID"`
	Driver        string `json:"driver"`
	Remote        string `json:"remote"`
}

func newNFSVolumeStager(stateDir string) *nfsVolumeStager {
	return &nfsVolumeStager{root: filepath.Join(stateDir, ".volumes"), mounts: hostMountOperations{}}
}

func nfsRemote(volume *hostruntimepb.DurableVolume) (string, error) {
	if volume.GetDriver() != nfsCSIDriver {
		return "", fmt.Errorf("durable volume %q uses unsupported driver %q; only %s is supported", volume.GetName(), volume.GetDriver(), nfsCSIDriver)
	}
	server := volume.GetVolumeContext()["server"]
	if ip := net.ParseIP(server); ip != nil {
		if strings.Contains(server, ":") {
			server = "[" + server + "]"
		}
	} else if !nfsServerPattern.MatchString(server) {
		return "", fmt.Errorf("durable volume %q has an invalid NFS server", volume.GetName())
	}
	share := volume.GetVolumeContext()["share"]
	if !cleanAbsoluteNFSPath(share) {
		return "", fmt.Errorf("durable volume %q has an invalid NFS share", volume.GetName())
	}
	subdir := volume.GetVolumeContext()["subdir"]
	if subdir != "" {
		if !cleanRelativeNFSPath(subdir) {
			return "", fmt.Errorf("durable volume %q has an invalid NFS subDir", volume.GetName())
		}
		share = path.Join(share, subdir)
	}
	return server + ":" + share, nil
}

func cleanAbsoluteNFSPath(value string) bool {
	return value != "" && strings.HasPrefix(value, "/") && path.Clean(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

func cleanRelativeNFSPath(value string) bool {
	return value != "." && value != ".." && !strings.HasPrefix(value, "/") && path.Clean(value) == value && !strings.HasPrefix(value, "../") && !strings.ContainsAny(value, "\x00\r\n")
}

func virtioFSTag(name, mountPath string) string {
	sum := sha256.Sum256([]byte(name + "\x00" + mountPath))
	return "ate-" + hex.EncodeToString(sum[:8])
}

func (s *nfsVolumeStager) Stage(ctx context.Context, actorUID string, volumes []*hostruntimepb.DurableVolume) (_ []stagedVolume, err error) {
	if validateUID(actorUID) != nil {
		return nil, errors.New("invalid Actor UID")
	}
	if len(volumes) == 0 {
		return nil, s.rejectUnexpectedReceipts(actorUID, nil)
	}
	actorRoot := filepath.Join(s.root, actorUID)
	if err := os.MkdirAll(actorRoot, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(actorRoot, 0o700); err != nil {
		return nil, err
	}

	desired := make(map[string]nfsStageReceipt, len(volumes))
	for _, volume := range volumes {
		if volume == nil || validateUID(volume.GetName()) != nil || volume.GetVolumeId() == "" {
			return nil, errors.New("invalid durable volume identity")
		}
		remote, err := nfsRemote(volume)
		if err != nil {
			return nil, err
		}
		receipt := nfsStageReceipt{SchemaVersion: 1, VolumeID: volume.GetVolumeId(), Driver: volume.GetDriver(), Remote: remote}
		if old, ok := desired[volume.GetName()]; ok && old != receipt {
			return nil, fmt.Errorf("durable volume %q has conflicting mount metadata", volume.GetName())
		}
		desired[volume.GetName()] = receipt
	}
	if err := s.rejectUnexpectedReceipts(actorUID, desired); err != nil {
		return nil, err
	}

	var mountedNow []string
	defer func() {
		if err == nil {
			return
		}
		for i := len(mountedNow) - 1; i >= 0; i-- {
			_ = s.mounts.Unmount(context.WithoutCancel(ctx), mountedNow[i])
		}
	}()
	paths := make(map[string]string, len(desired))
	for name, receipt := range desired {
		target := filepath.Join(actorRoot, name)
		if err := os.Mkdir(target, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		exists, err := safeDirectory(target)
		if err != nil {
			return nil, fmt.Errorf("durable volume staging path is unsafe: %w", err)
		}
		if !exists {
			return nil, errors.New("durable volume staging path is unsafe")
		}
		if err := ensureStageReceipt(filepath.Join(actorRoot, name+".json"), receipt); err != nil {
			return nil, err
		}
		mounted, err := s.mounts.Mounted(ctx, target)
		if err != nil {
			return nil, fmt.Errorf("inspect NFS mount for %q: %w", name, err)
		}
		if !mounted {
			if err := s.mounts.MountNFS(ctx, receipt.Remote, target); err != nil {
				return nil, fmt.Errorf("stage durable volume %q: %w", name, err)
			}
			mountedNow = append(mountedNow, target)
		}
		paths[name] = target
	}

	staged := make([]stagedVolume, 0, len(volumes))
	for _, volume := range volumes {
		staged = append(staged, stagedVolume{
			Name: volume.GetName(), MountPath: volume.GetMountPath(),
			Tag: virtioFSTag(volume.GetName(), volume.GetMountPath()), HostPath: paths[volume.GetName()],
		})
	}
	return staged, nil
}

func (s *nfsVolumeStager) rejectUnexpectedReceipts(actorUID string, desired map[string]nfsStageReceipt) error {
	actorRoot := filepath.Join(s.root, actorUID)
	entries, err := os.ReadDir(actorRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".json")
		if _, ok := desired[name]; !ok {
			return fmt.Errorf("Actor has staged durable volume %q not present in activation request", name)
		}
	}
	return nil
}

func ensureStageReceipt(receiptPath string, want nfsStageReceipt) error {
	b, err := os.ReadFile(receiptPath)
	if err == nil {
		var got nfsStageReceipt
		if json.Unmarshal(b, &got) != nil || got != want {
			return errors.New("durable volume activation conflicts with its persisted stage receipt")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return writeJSONAtomic(receiptPath, want)
}

func (s *nfsVolumeStager) Unstage(ctx context.Context, actorUID string) error {
	actorRoot := filepath.Join(s.root, actorUID)
	entries, err := os.ReadDir(actorRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var errs []error
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".json")
		target := filepath.Join(actorRoot, name)
		mounted, mountErr := s.mounts.Mounted(ctx, target)
		if mountErr != nil {
			errs = append(errs, fmt.Errorf("inspect durable volume %q: %w", name, mountErr))
			continue
		}
		if mounted {
			if err := s.mounts.Unmount(ctx, target); err != nil {
				errs = append(errs, fmt.Errorf("unstage durable volume %q: %w", name, err))
				continue
			}
		}
		if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
			continue
		}
		if err := os.Remove(filepath.Join(actorRoot, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		if err := os.Remove(actorRoot); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func writeVMVolumeConfig(bundle string, volumes []stagedVolume) error {
	hostConfig := filepath.Join(bundle, vmVolumeConfigName)
	guestDir := filepath.Join(bundle, guestConfigDirectory)
	// VirtioFS preserves host permissions and presents the host owner as an
	// unknown guest user. The guest agent therefore needs search permission on
	// this directory even though the share itself is read-only.
	if err := os.MkdirAll(guestDir, 0o755); err != nil {
		return err
	}
	if err := os.Chmod(guestDir, 0o755); err != nil {
		return err
	}
	if len(volumes) == 0 {
		removeErr := os.Remove(hostConfig)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		if err := writeJSONAtomic(filepath.Join(guestDir, "volumes.json"), []guestVolume{}); err != nil {
			return errors.Join(removeErr, err)
		}
		return removeErr
	}
	guest := make([]guestVolume, 0, len(volumes))
	for _, volume := range volumes {
		guest = append(guest, guestVolume{Name: volume.Name, MountPath: volume.MountPath, Tag: volume.Tag})
	}
	if err := writeJSONAtomic(hostConfig, volumes); err != nil {
		return err
	}
	return writeJSONAtomic(filepath.Join(guestDir, "volumes.json"), guest)
}
