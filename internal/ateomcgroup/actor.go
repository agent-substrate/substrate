//go:build linux

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

package ateomcgroup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	// cpuPeriodUS is the CFS period the CPU quota is expressed against.
	cpuPeriodUS = 100000
	// cpuMinQuotaUS is the smallest quota the kernel accepts.
	cpuMinQuotaUS = 1000
)

// ActorLeaf is a cgroup of its own for one actor's host processes, so actors on
// one worker share its CPU fairly and each stays within its declared limit.
type ActorLeaf struct {
	dir *os.File
}

// OpenActorLeaf creates the actor's leaf under the delegated scope, or reuses a
// leftover one, and caps it at milliCPU when that is set. Close it once its
// processes are started.
func OpenActorLeaf(actorUID string, milliCPU int64) (*ActorLeaf, error) {
	return openActorLeaf(Root, actorUID, milliCPU)
}

func openActorLeaf(root, actorUID string, milliCPU int64) (*ActorLeaf, error) {
	path, err := actorLeafPath(root, actorUID)
	if err != nil {
		return nil, err
	}
	if err := os.Mkdir(path, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, fmt.Errorf("while creating cgroup %q: %w", path, err)
	}
	// Written even when unset, so a reused leaf does not keep an old cap. A
	// node that did not delegate the cpu controller has no cpu.max to write.
	if err := writeExisting(filepath.Join(path, "cpu.max"), cpuMax(milliCPU)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("while limiting cgroup %q: %w", path, err)
	}
	dir, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("while opening cgroup %q: %w", path, err)
	}
	return &ActorLeaf{dir: dir}, nil
}

// writeExisting writes a cgroup interface file without creating it: cgroupfs
// refuses creation with EACCES, which would hide a file that is simply absent.
func writeExisting(path, value string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return err
	}
	_, err = f.WriteString(value)
	return errors.Join(err, f.Close())
}

// cpuMax renders a cgroup v2 cpu.max value for a limit in millicores.
func cpuMax(milliCPU int64) string {
	if milliCPU <= 0 {
		return fmt.Sprintf("max %d", cpuPeriodUS)
	}
	return fmt.Sprintf("%d %d", max(milliCPU*cpuPeriodUS/1000, cpuMinQuotaUS), cpuPeriodUS)
}

// SysProcAttr starts a process directly inside the leaf. A nil leaf starts it
// where the caller is.
func (l *ActorLeaf) SysProcAttr() *syscall.SysProcAttr {
	if l == nil {
		return nil
	}
	return &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(l.dir.Fd())}
}

// Close releases the leaf's handle. The leaf itself stays until
// RemoveActorLeaf.
func (l *ActorLeaf) Close() error {
	if l == nil {
		return nil
	}
	return l.dir.Close()
}

// KillActorLeaf kills every process in the named leaf, including any they
// forked, and waits until the leaf is empty or ctx is done. A missing leaf, or a
// kernel without cgroup.kill, has nothing done.
func KillActorLeaf(ctx context.Context, name string) error {
	return KillLeafUnder(ctx, Root, name)
}

// KillLeafUnder is KillActorLeaf for a leaf under root rather than Root.
func KillLeafUnder(ctx context.Context, root, name string) error {
	path, err := actorLeafPath(root, name)
	if err != nil {
		return err
	}
	if err := writeExisting(filepath.Join(path, "cgroup.kill"), "1"); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("while killing cgroup %q: %w", path, err)
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		events, err := os.ReadFile(filepath.Join(path, "cgroup.events"))
		if err != nil {
			return fmt.Errorf("while waiting for cgroup %q to empty: %w", path, err)
		}
		if strings.Contains(string(events), "populated 0") {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("while waiting for cgroup %q to empty: %w", path, ctx.Err())
		case <-ticker.C:
		}
	}
}

// RemoveActorLeaf deletes the actor's leaf. Its processes must have exited.
func RemoveActorLeaf(actorUID string) error {
	return removeActorLeaf(Root, actorUID)
}

func removeActorLeaf(root, actorUID string) error {
	path, err := actorLeafPath(root, actorUID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("while removing cgroup %q: %w", path, err)
	}
	return nil
}

func actorLeafPath(root, actorUID string) (string, error) {
	if actorUID == "" || actorUID == "." || actorUID == ".." || actorUID == workerLeaf || strings.ContainsAny(actorUID, "/\x00") {
		return "", fmt.Errorf("invalid actor cgroup name %q", actorUID)
	}
	return filepath.Join(root, actorUID), nil
}
