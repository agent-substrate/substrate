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

package cgroup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
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
// leftover one, caps it at milliCPU and sets memory.high to memoryHigh bytes
// when those are set. Close it once its processes are started.
func OpenActorLeaf(actorUID string, milliCPU, memoryHigh int64) (*ActorLeaf, error) {
	return openActorLeaf(Root, actorUID, milliCPU, memoryHigh)
}

func openActorLeaf(root, actorUID string, milliCPU, memoryHigh int64) (*ActorLeaf, error) {
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
	high := "max"
	if memoryHigh > 0 {
		high = strconv.FormatInt(memoryHigh, 10)
	}
	if err := writeExisting(filepath.Join(path, "memory.high"), high); err != nil && !errors.Is(err, fs.ErrNotExist) {
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

// SysProcAttr starts a process directly inside the leaf.
func (l *ActorLeaf) SysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(l.dir.Fd())}
}

// Close releases the leaf's handle. The leaf itself stays until
// RemoveActorLeaf.
func (l *ActorLeaf) Close() error {
	return l.dir.Close()
}

// LimitActorLeafGrowth sets memory.high on the actor's leaf to ceiling bytes, or
// to its current usage plus headroom bytes if that is more, plus borrow bytes,
// and returns a func that puts back the memory.high it replaced. Past
// memory.high the kernel reclaims the leaf's page cache and throttles whoever
// is allocating, where at the worker pod's memory.max it may OOM-kill instead.
func LimitActorLeafGrowth(actorUID string, headroom, ceiling, borrow int64) (func(), error) {
	return limitLeafGrowth(Root, actorUID, headroom, ceiling, borrow)
}

func limitLeafGrowth(root, actorUID string, headroom, ceiling, borrow int64) (func(), error) {
	current, err := actorLeafMemory(root, actorUID)
	if err != nil {
		return nil, err
	}
	return setLeafMemoryHigh(root, actorUID, strconv.FormatInt(max(current+headroom, ceiling)+borrow, 10))
}

// LiftActorLeafMemory clears memory.high on the actor's leaf, and returns a
// func that puts back the memory.high it replaced.
func LiftActorLeafMemory(actorUID string) (func(), error) {
	return setLeafMemoryHigh(Root, actorUID, "max")
}

func setLeafMemoryHigh(root, actorUID, value string) (func(), error) {
	path, err := actorLeafPath(root, actorUID)
	if err != nil {
		return nil, err
	}
	high := filepath.Join(path, "memory.high")
	previous, err := os.ReadFile(high)
	if err != nil {
		return nil, fmt.Errorf("while limiting cgroup %q: %w", path, err)
	}
	if err := writeExisting(high, value); err != nil {
		return nil, fmt.Errorf("while limiting cgroup %q: %w", path, err)
	}
	return func() {
		// the leaf may already be gone, and its limit with it
		_ = writeExisting(high, strings.TrimSpace(string(previous)))
	}, nil
}

// ActorLeafMemory returns the actor's leaf memory.current.
func ActorLeafMemory(actorUID string) (int64, error) {
	return actorLeafMemory(Root, actorUID)
}

func actorLeafMemory(root, actorUID string) (int64, error) {
	path, err := actorLeafPath(root, actorUID)
	if err != nil {
		return 0, err
	}
	raw, err := os.ReadFile(filepath.Join(path, "memory.current"))
	if err != nil {
		return 0, fmt.Errorf("while reading cgroup %q memory usage: %w", path, err)
	}
	current, err := parseBytes(raw)
	if err != nil {
		return 0, fmt.Errorf("while parsing cgroup %q memory usage %q: %w", path, raw, err)
	}
	return current, nil
}

// PodMemory returns the worker pod's memory limit and what it uses besides
// clean page cache, which the kernel can reclaim. limit is 0 when the pod has
// none.
func PodMemory() (limit, used int64, err error) {
	return podMemory(Root)
}

func podMemory(root string) (limit, used int64, err error) {
	raw, err := os.ReadFile(filepath.Join(root, "memory.max"))
	if err != nil {
		return 0, 0, err
	}
	if v := strings.TrimSpace(string(raw)); v != "max" {
		if limit, err = parseBytes(raw); err != nil {
			return 0, 0, fmt.Errorf("while parsing memory.max %q: %w", raw, err)
		}
	}
	used, err = memoryUnreclaimable(root)
	if err != nil {
		return 0, 0, err
	}
	return limit, used, nil
}

// memoryUnreclaimable is memoryUsed plus page cache that is dirty or under
// writeback, which reclaim can't drop either.
func memoryUnreclaimable(root string) (int64, error) {
	used, err := sumMemoryStat(root, slices.Concat(memoryUsedStats, []string{"file_dirty", "file_writeback"})...)
	return int64(used), err
}

func parseBytes(raw []byte) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
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
