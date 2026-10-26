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

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/cmd/ateworker-kata/internal/shim"
)

const (
	// blockRootfsHeadroom is the share of the rootfs size, and blockRootfsMin the
	// bytes, that an image has free for the container's own writes.
	blockRootfsHeadroom = 0.5
	blockRootfsMin      = 256 << 20
	// blockSize is the block size of the image, which rounds up each file.
	blockSize = 4096
)

// blockRootfs is a container's rootfs as an ext4 image on a loop device. A
// hypervisor with no shared filesystem, such as Firecracker, takes the rootfs
// as a block device. The container's writes land in the image and go with it.
type blockRootfs struct {
	image  string
	device string
}

// newBlockRootfs copies the rootfs directory of the bundle into an ext4 image
// next to it and attaches the image to a loop device. It needs mke2fs, and
// access to the loop devices.
func newBlockRootfs(ctx context.Context, bundle string) (_ *blockRootfs, retErr error) {
	rootfs := filepath.Join(bundle, "rootfs")
	used, entries, err := treeSize(rootfs)
	if err != nil {
		return nil, err
	}
	image := filepath.Join(bundle, "rootfs.img")
	if err := os.WriteFile(image, nil, 0o600); err != nil {
		return nil, err
	}
	b := &blockRootfs{image: image}
	defer func() {
		if retErr != nil {
			_ = b.close()
		}
	}()
	if err := os.Truncate(image, imageBytes(used)); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	// mke2fs makes one inode for each 16 KiB of image, which a rootfs of many
	// small files outruns. Leave room for the container's own files too.
	inodes := entries + entries/2 + 1024
	cmd := exec.CommandContext(ctx, "mke2fs", "-q", "-F", "-t", "ext4", "-N", strconv.FormatInt(inodes, 10), "-d", rootfs, image)
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("building the ext4 rootfs image: %w: %s", err, bytes.TrimSpace(out.Bytes()))
	}
	if b.device, err = attachLoop(image); err != nil {
		return nil, fmt.Errorf("attaching the rootfs image to a loop device: %w", err)
	}
	return b, nil
}

// mount is the rootfs mount the shim takes in place of the rootfs directory.
func (b *blockRootfs) mount() shim.Mount {
	return shim.Mount{Type: "ext4", Source: b.device}
}

// close detaches the loop device and removes the image. The device goes once
// the VM that has it open is gone.
func (b *blockRootfs) close() error {
	var errs []error
	if b.device != "" {
		errs = append(errs, detachLoop(b.device))
	}
	if err := os.Remove(b.image); err != nil && !errors.Is(err, fs.ErrNotExist) {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// imageBytes returns the size of the image for a rootfs of used bytes.
func imageBytes(used int64) int64 {
	return max(blockRootfsMin, used+int64(float64(used)*blockRootfsHeadroom))
}

// treeSize returns the bytes the regular files under dir take in blocks, and
// the number of entries under it.
func treeSize(dir string) (bytes, entries int64, _ error) {
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		entries++
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			bytes += (info.Size() + blockSize - 1) / blockSize * blockSize
		}
		return nil
	})
	return bytes, entries, err
}

// attachLoop attaches image to a free loop device and returns the device. The
// device it picked can be taken by another process before it is configured, in
// which case it tries again.
func attachLoop(image string) (string, error) {
	ctl, err := os.OpenFile("/dev/loop-control", os.O_RDWR, 0)
	if err != nil {
		return "", err
	}
	defer ctl.Close()
	img, err := os.OpenFile(image, os.O_RDWR, 0)
	if err != nil {
		return "", err
	}
	defer img.Close()
	for range 8 {
		n, err := unix.IoctlRetInt(int(ctl.Fd()), unix.LOOP_CTL_GET_FREE)
		if err != nil {
			return "", err
		}
		device := fmt.Sprintf("/dev/loop%d", n)
		dev, err := os.OpenFile(device, os.O_RDWR, 0)
		if err != nil {
			return "", err
		}
		err = unix.IoctlLoopConfigure(int(dev.Fd()), &unix.LoopConfig{Fd: uint32(img.Fd())})
		dev.Close()
		if err == nil {
			return device, nil
		}
		if !errors.Is(err, unix.EBUSY) {
			return "", err
		}
	}
	return "", errors.New("no free loop device")
}

// detachLoop detaches the loop device. One still open elsewhere is cleared
// when the last user closes it.
func detachLoop(device string) error {
	dev, err := os.OpenFile(device, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer dev.Close()
	if err := unix.IoctlSetInt(int(dev.Fd()), unix.LOOP_CLR_FD, 0); err != nil && !errors.Is(err, unix.ENXIO) {
		return fmt.Errorf("detaching %s: %w", device, err)
	}
	return nil
}

// detachLoopsUnder detaches the loop devices whose backing file is under dir.
// It clears what a worker that died left attached, which nothing else holds.
func detachLoopsUnder(dir string) error {
	files, err := filepath.Glob("/sys/block/loop*/loop/backing_file")
	if err != nil {
		return err
	}
	var errs []error
	for _, file := range files {
		b, err := os.ReadFile(file)
		if err != nil {
			// The device was detached since the listing.
			continue
		}
		backing := strings.TrimSuffix(strings.TrimSpace(string(b)), " (deleted)")
		if !strings.HasPrefix(backing, dir+string(filepath.Separator)) {
			continue
		}
		device := filepath.Join("/dev", filepath.Base(filepath.Dir(filepath.Dir(file))))
		if err := detachLoop(device); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
