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

// Upper disks put a container's writable rootfs upper on its own virtio-blk
// ext4 disk. The RootfsUpperDisks preview gate turns them on for cold boots.
// A checkpoint then ships one image file in place of the upper tar:
//
//   - Cold boot writes the SandboxConfig's rootfs-upper-disk asset, an empty
//     ext4 image, to <upperBase>/<name>.upper.ext4 and gives it a fresh UUID.
//     The share serves the bare image at <cid>/rootfs, and the guest mounts
//     the disk and the overlay (kata.UpperDisk).
//   - Checkpoint hard-links the image into the checkpoint dir as
//     rootfs-disk-<name>.ext4. A MEMORY snapshot holds the guest's dirty ext4
//     pages in memory-ranges, so a ROOTFS restore of it restores at VOLUMES
//     instead. A ROOTFS checkpoint powers the guest off first (powerOffGuest).
//   - Restore copies the image back and points the snapshot config's disks at
//     this actor's upper dir.
//
// A restore puts back whichever form its snapshot has.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ext4uuid"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

const (
	// upperDiskSuffix names the host image beside the container's upper dir.
	upperDiskSuffix = ".upper.ext4"
	// rootfsDiskPrefix names the image in a snapshot.
	rootfsDiskPrefix = "rootfs-disk-"
)

// upperDiskPath is the host image backing container name's upper.
func upperDiskPath(upperBase, name string) string {
	return filepath.Join(upperBase, name+upperDiskSuffix)
}

// rootfsDiskFile is the snapshot file holding container name's upper image.
func rootfsDiskFile(name string) (string, error) {
	if name == "" || name == "." || strings.Contains(name, "/") || !filepath.IsLocal(name) {
		return "", fmt.Errorf("invalid container name %q", name)
	}
	return rootfsDiskPrefix + name + ".ext4", nil
}

// hasUpperDisk reports whether container name's upper is on a disk.
func hasUpperDisk(upperBase, name string) bool {
	_, err := os.Stat(upperDiskPath(upperBase, name))
	return err == nil
}

// makeUpperDisks writes an upper disk for each container from the zstd image
// at imagePath. Each disk gets a random UUID so no two disks in a guest share
// one.
func makeUpperDisks(upperBase, imagePath string, containers []string) error {
	if len(containers) > kata.MaxUpperDisks {
		return fmt.Errorf("%d containers need upper disks, more than the %d a microVM supports", len(containers), kata.MaxUpperDisks)
	}
	img, err := loadUpperDiskImage(imagePath)
	if err != nil {
		return fmt.Errorf("while loading the upper disk image %q: %w", imagePath, err)
	}
	for _, name := range containers {
		if _, err := rootfsDiskFile(name); err != nil {
			return err
		}
		path := upperDiskPath(upperBase, name)
		if err := img.write(path); err != nil {
			_ = os.Remove(path)
			return fmt.Errorf("while writing the upper disk of %q: %w", name, err)
		}
		if err := ext4uuid.SetUUID(path, [16]byte(uuid.New())); err != nil {
			_ = os.Remove(path)
			return fmt.Errorf("while setting the UUID of the upper disk of %q: %w", name, err)
		}
	}
	return nil
}

// sparseImage is a disk image held as its non-zero extents.
type sparseImage struct {
	size    int64
	extents []imageExtent
}

type imageExtent struct {
	off  int64
	data []byte
}

// imageBlock is the granularity at which zero blocks become holes.
const imageBlock = 4096

// loadUpperDiskImage decompresses the zstd image at path, keeping its non-zero
// blocks, and takes the size from the superblock.
func loadUpperDiskImage(path string) (*sparseImage, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	zr, err := zstd.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	img, err := readSparse(zr)
	if err != nil {
		return nil, err
	}
	if len(img.extents) == 0 || img.extents[0].off != 0 || len(img.extents[0].data) < 2048 {
		return nil, errors.New("no ext4 superblock")
	}
	size, err := ext4Size(img.extents[0].data[1024:2048])
	if err != nil {
		return nil, err
	}
	if size < img.size {
		return nil, fmt.Errorf("image is %d bytes, larger than its %d-byte filesystem", img.size, size)
	}
	img.size = size
	return img, nil
}

// readSparse reads r into its non-zero extents.
func readSparse(r io.Reader) (*sparseImage, error) {
	img := &sparseImage{}
	buf := make([]byte, 1<<20)
	zero := make([]byte, imageBlock)
	for {
		n, err := io.ReadFull(r, buf)
		for b := 0; b < n; b += imageBlock {
			blk := buf[b:min(b+imageBlock, n)]
			if bytes.Equal(blk, zero[:len(blk)]) {
				continue
			}
			off := img.size + int64(b)
			if k := len(img.extents); k > 0 && img.extents[k-1].off+int64(len(img.extents[k-1].data)) == off {
				img.extents[k-1].data = append(img.extents[k-1].data, blk...)
			} else {
				img.extents = append(img.extents, imageExtent{off: off, data: bytes.Clone(blk)})
			}
		}
		img.size += int64(n)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return img, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// write creates a sparse file at path holding the image.
func (img *sparseImage) write(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(img.size); err != nil {
		return err
	}
	for _, e := range img.extents {
		if _, err := f.WriteAt(e.data, e.off); err != nil {
			return err
		}
	}
	return f.Close()
}

// ext4Size returns the filesystem size in an ext4 superblock. See struct
// ext4_super_block in fs/ext4/ext4.h.
func ext4Size(sb []byte) (int64, error) {
	if binary.LittleEndian.Uint16(sb[0x38:]) != 0xef53 {
		return 0, errors.New("not an ext4 filesystem")
	}
	blocks := uint64(binary.LittleEndian.Uint32(sb[0x4:]))
	if binary.LittleEndian.Uint32(sb[0x60:])&0x80 != 0 { // INCOMPAT_64BIT
		blocks |= uint64(binary.LittleEndian.Uint32(sb[0x150:])) << 32
	}
	logBlock := binary.LittleEndian.Uint32(sb[0x18:])
	if logBlock > 6 {
		return 0, fmt.Errorf("unsupported ext4 block size 2^%d KiB", logBlock)
	}
	return int64(blocks) * (1024 << logBlock), nil
}

// upperDisks returns the VM's virtio-blk disks and, by container name, the
// guest mount of each.
func upperDisks(upperBase string, ctrs []actorContainer) ([]ch.DiskConfig, map[string]*kata.UpperDisk, error) {
	var disks []ch.DiskConfig
	mounts := map[string]*kata.UpperDisk{}
	for _, c := range ctrs {
		if !hasUpperDisk(upperBase, c.name) {
			continue
		}
		m, err := kata.UpperDiskMount(len(disks), c.id)
		if err != nil {
			return nil, nil, err
		}
		// Segment 1 with the virtio-fs device keeps segment 0's slots for
		// the VM's core devices. Its 31 slots fit virtio-fs and every disk.
		// Direct keeps the guest's writes out of the host page cache, which
		// counts against the actor's memory.high (cgroup.MemoryGuard). The
		// guest caches the disk itself.
		disks = append(disks, ch.DiskConfig{Path: upperDiskPath(upperBase, c.name), Direct: true, ImageType: "Raw", NumQueues: 1, QueueSize: 256, PciSegment: 1})
		mounts[c.name] = &m
	}
	return disks, mounts, nil
}

// sandboxUpperDisks lists the guest mounts in disk order.
func sandboxUpperDisks(ctrs []actorContainer, mounts map[string]*kata.UpperDisk) []kata.UpperDisk {
	var out []kata.UpperDisk
	for _, c := range ctrs {
		if m := mounts[c.name]; m != nil {
			out = append(out, *m)
		}
	}
	return out
}

// checkUpperDiskSupport rejects booting with upper disks when the guest does
// not run the agent as PID 1, which powerOffGuest needs to sync them.
func checkUpperDiskSupport(disks []ch.DiskConfig, agentIsInit bool) error {
	if len(disks) > 0 && !agentIsInit {
		return errors.New("rootfs upper disks require cloud-hypervisor >= 53.0 (kata-agent as PID 1)")
	}
	return nil
}

// restorableFidelity returns the fidelity a restore of the snapshot in
// restoreDir runs at. A ROOTFS restore of a MEMORY snapshot with an upper disk
// runs at VOLUMES: a MEMORY checkpoint pauses the guest without syncing its
// upper disks, so their dirty ext4 pages live only in memory-ranges.
func restorableFidelity(ctx context.Context, want ateworkerpb.SnapshotFidelity, restoreDir string, containers []string) (ateworkerpb.SnapshotFidelity, error) {
	if want != ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS {
		return want, nil
	}
	memSnap := false
	for _, f := range []string{memoryRangesFile, "state.json", "config.json"} {
		if _, err := os.Stat(filepath.Join(restoreDir, f)); err == nil {
			memSnap = true
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
	}
	if !memSnap {
		return want, nil
	}
	for _, name := range containers {
		file, err := rootfsDiskFile(name)
		if err != nil {
			return 0, err
		}
		if _, err := os.Stat(filepath.Join(restoreDir, file)); err == nil {
			slog.WarnContext(ctx, "Restoring at VOLUMES: the MEMORY snapshot's upper disk is only consistent with its memory",
				slog.String("container", name))
			return ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
	}
	return want, nil
}

// linkUpperDisk hard-links container name's upper image into checkpointDir and
// reports whether it had one. The actor is torn down after the checkpoint, so
// nothing writes to the image again. It is not fsynced, like the tars.
func linkUpperDisk(ctx context.Context, upperBase, checkpointDir, name string) (bool, error) {
	if !hasUpperDisk(upperBase, name) {
		return false, nil
	}
	file, err := rootfsDiskFile(name)
	if err != nil {
		return true, err
	}
	src, dst := upperDiskPath(upperBase, name), filepath.Join(checkpointDir, file)
	if err := os.Link(src, dst); errors.Is(err, unix.EXDEV) {
		return true, copySparse(ctx, src, dst)
	} else if err != nil {
		return true, fmt.Errorf("while linking upper disk %q: %w", src, err)
	}
	return true, nil
}

// restoreUpperDisk copies container name's upper image out of snapshotDir and
// reports whether the snapshot had one. It copies because the guest writes to
// the image and snapshotDir may be a preserved snapshot.
func restoreUpperDisk(ctx context.Context, upperBase, snapshotDir, name string) (bool, error) {
	file, err := rootfsDiskFile(name)
	if err != nil {
		return false, err
	}
	src := filepath.Join(snapshotDir, file)
	if _, err := os.Stat(src); errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, copySparse(ctx, src, upperDiskPath(upperBase, name))
}

// copySparse copies src's data extents to a new file dst. It uses
// copy_file_range, which shares extents on XFS and btrfs, where ch's
// copySparseFile reads and writes. It falls back to read and write where the
// kernel cannot copy between the two files. It removes dst if the copy fails.
func copySparse(ctx context.Context, src, dst string) (retErr error) {
	s, err := os.Open(src)
	if err != nil {
		return err
	}
	defer s.Close()
	si, err := s.Stat()
	if err != nil {
		return err
	}
	d, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer d.Close()
	defer func() {
		if retErr != nil {
			_ = os.Remove(dst)
		}
	}()
	if err := d.Truncate(si.Size()); err != nil {
		return err
	}
	sfd, dfd := int(s.Fd()), int(d.Fd())
	useKernel := true
	for off := int64(0); off < si.Size(); {
		if err := ctx.Err(); err != nil {
			return err
		}
		ds, err := unix.Seek(sfd, off, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			break
		} else if err != nil {
			return fmt.Errorf("SEEK_DATA: %w", err)
		}
		de, err := unix.Seek(sfd, ds, unix.SEEK_HOLE)
		if err != nil {
			return fmt.Errorf("SEEK_HOLE: %w", err)
		}
		pos := ds
		for useKernel && pos < de {
			roff, woff := pos, pos
			n, err := unix.CopyFileRange(sfd, &roff, dfd, &woff, int(min(de-pos, 1<<30)), 0)
			switch {
			case errors.Is(err, unix.EINTR):
			case errors.Is(err, unix.EXDEV), errors.Is(err, unix.EINVAL), errors.Is(err, unix.EOPNOTSUPP), errors.Is(err, unix.ENOSYS):
				useKernel = false
			case err != nil:
				return fmt.Errorf("copy_file_range: %w", err)
			case n == 0:
				return fmt.Errorf("copy_file_range: short copy at %d", pos)
			default:
				pos += int64(n)
			}
		}
		if pos < de {
			if _, err := io.Copy(io.NewOffsetWriter(d, pos), io.NewSectionReader(s, pos, de-pos)); err != nil {
				return err
			}
		}
		off = de
	}
	return d.Close()
}

// powerOffGuest has the guest sync its upper disks and power off, for a ROOTFS
// checkpoint. A ROOTFS restore cold-boots on the images, so they must hold the
// guest's dirty ext4 pages. The agent has no call that syncs or unmounts a
// sandbox storage. DestroySandbox stops the agent, and the agent, as PID 1,
// calls sync(2) and powers the VM off before it exits.
//
// ra.stopping keeps watchVM from reporting the power-off as a crash. It is
// cleared only if DialAgent fails before DestroySandbox starts powering the
// guest off, and a retry returns once an earlier DestroySandbox has shut the
// guest down.
func powerOffGuest(ctx context.Context, vsockPath string, client *ch.Client, ra *runningActor) (retErr error) {
	if ra.isGuestPoweredOff() {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if ra != nil && ra.poweringOff.Load() {
		if st, err := client.State(ctx); err == nil && st == "Shutdown" {
			ra.guestPoweredOff.Store(true)
			return nil
		}
	}
	if ra != nil {
		ra.stopping.Store(true)
		defer func() {
			if retErr != nil && !ra.poweringOff.Load() {
				ra.stopping.Store(false)
			}
		}()
	}
	ac, err := kata.DialAgent(ctx, vsockPath)
	if err != nil {
		return fmt.Errorf("while dialing the agent to power off the guest: %w", err)
	}
	if ra != nil {
		ra.poweringOff.Store(true)
	}
	err = ac.DestroySandbox(ctx)
	_ = ac.Close()
	if err != nil {
		return fmt.Errorf("while powering off the guest: %w", err)
	}
	var exited <-chan struct{}
	if ra != nil && ra.chProc != nil {
		exited = ra.chProc.Done()
	}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-exited:
			if ra != nil {
				ra.guestPoweredOff.Store(true)
			}
			return nil
		case <-ctx.Done():
			return fmt.Errorf("guest did not power off: %w", ctx.Err())
		case <-tick.C:
		}
		if st, err := client.State(ctx); err == nil && st == "Shutdown" {
			if ra != nil {
				ra.guestPoweredOff.Store(true)
			}
			return nil
		}
	}
}

// rewriteUpperDiskPaths points the upper disks in a snapshot config at
// upperBase and reports whether it changed any.
func rewriteUpperDiskPaths(cfg map[string]any, cfgPath, upperBase string) (bool, error) {
	var disks []any
	if v, ok := cfg["disks"]; ok {
		if disks, ok = v.([]any); !ok {
			return false, fmt.Errorf("snapshot config %q has malformed disks", cfgPath)
		}
	}
	changed := false
	for _, dk := range disks {
		dm, ok := dk.(map[string]any)
		if !ok {
			return false, fmt.Errorf("snapshot config %q has a malformed disk", cfgPath)
		}
		p, _ := dm["path"].(string)
		if !strings.HasSuffix(p, upperDiskSuffix) {
			return false, fmt.Errorf("snapshot config %q has disk %q, which is not an upper disk", cfgPath, p)
		}
		if want := filepath.Join(upperBase, filepath.Base(p)); want != p {
			dm["path"] = want
			changed = true
		}
	}
	return changed, nil
}
