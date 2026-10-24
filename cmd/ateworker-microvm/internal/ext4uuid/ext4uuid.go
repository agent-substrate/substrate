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

// Package ext4uuid sets the UUID of an unmounted ext4 image, so copies of one
// golden image get their own UUID without e2fsprogs
//
// This is the offline equivalent of `tune2fs -U` for images with
// metadata_csum_seed, where only the superblock copies change. Without the
// seed, metadata checksums are keyed on the UUID and every one of them would
// need rewriting, so those images are rejected.
package ext4uuid

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// superblock layout, see struct ext4_super_block in fs/ext4/ext4.h
const (
	superblockOffset = 1024
	superblockSize   = 1024

	offBlocksCountLo   = 0x4
	offFirstDataBlock  = 0x14
	offLogBlockSize    = 0x18
	offBlocksPerGroup  = 0x20
	offMagic           = 0x38
	offFeatureCompat   = 0x5c
	offFeatureIncompat = 0x60
	offFeatureRoCompat = 0x64
	offUUID            = 0x68
	offBlocksCountHi   = 0x150
	offBackupBGs       = 0x24c
	offChecksum        = 0x3fc

	magic = 0xef53

	compatSparseSuper2   = 0x200
	compatStableInodes   = 0x800
	roCompatSparseSuper  = 0x1
	roCompatGdtCsum      = 0x10
	roCompatMetadataCsum = 0x400
	incompatJournalDev   = 0x8
	incompat64Bit        = 0x80
	incompatEAInode      = 0x400
	incompatCsumSeed     = 0x2000
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type readerWriterAt interface {
	io.ReaderAt
	io.WriterAt
}

// SetUUID writes uuid into every superblock copy of the ext4 image at path
// The image must not be mounted.
func SetUUID(path string, uuid [16]byte) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := setUUID(f, uuid); err != nil {
		f.Close()
		return fmt.Errorf("while setting the UUID of %q: %w", path, err)
	}
	return f.Close()
}

func setUUID(f readerWriterAt, uuid [16]byte) error {
	sb := make([]byte, superblockSize)
	if _, err := f.ReadAt(sb, superblockOffset); err != nil {
		return fmt.Errorf("while reading the superblock: %w", err)
	}
	if binary.LittleEndian.Uint16(sb[offMagic:]) != magic {
		return errors.New("not an ext4 filesystem")
	}
	compat := binary.LittleEndian.Uint32(sb[offFeatureCompat:])
	incompat := binary.LittleEndian.Uint32(sb[offFeatureIncompat:])
	roCompat := binary.LittleEndian.Uint32(sb[offFeatureRoCompat:])
	// the same checks as tune2fs -U, except we don't rewrite checksums
	switch {
	case compat&compatStableInodes != 0:
		return errors.New("stable_inodes is set, inode numbers would change meaning")
	case incompat&incompatJournalDev != 0:
		return errors.New("external journal devices are not supported")
	case incompat&incompatCsumSeed == 0 && (roCompat&roCompatMetadataCsum != 0 || incompat&incompatEAInode != 0):
		return errors.New("metadata_csum without metadata_csum_seed needs every checksum rewritten, use tune2fs")
	case roCompat&roCompatGdtCsum != 0:
		return errors.New("gdt_csum group descriptor checksums are keyed on the UUID, use tune2fs")
	}

	blockSize := int64(1024) << binary.LittleEndian.Uint32(sb[offLogBlockSize:])
	blocks := int64(binary.LittleEndian.Uint32(sb[offBlocksCountLo:]))
	if incompat&incompat64Bit != 0 {
		blocks |= int64(binary.LittleEndian.Uint32(sb[offBlocksCountHi:])) << 32
	}
	firstDataBlock := int64(binary.LittleEndian.Uint32(sb[offFirstDataBlock:]))
	blocksPerGroup := int64(binary.LittleEndian.Uint32(sb[offBlocksPerGroup:]))
	if blocksPerGroup == 0 {
		return errors.New("corrupt superblock: zero blocks per group")
	}
	groups := (blocks - firstDataBlock + blocksPerGroup - 1) / blocksPerGroup
	backups := [2]int64{
		int64(binary.LittleEndian.Uint32(sb[offBackupBGs:])),
		int64(binary.LittleEndian.Uint32(sb[offBackupBGs+4:])),
	}
	csum := roCompat&roCompatMetadataCsum != 0

	// backups first, so a failure leaves the primary untouched
	for g := groups - 1; g >= 0; g-- {
		if !hasSuper(g, compat, roCompat, backups) {
			continue
		}
		off := int64(superblockOffset)
		if g > 0 {
			off = (firstDataBlock + g*blocksPerGroup) * blockSize
		}
		if err := patch(f, off, uuid, csum); err != nil {
			return fmt.Errorf("superblock in group %d: %w", g, err)
		}
	}
	return nil
}

// patch rewrites the UUID and checksum of the superblock at off, refusing to
// touch one that doesn't verify, like the kernel's EXT4_IOC_SETFSUUID
func patch(f readerWriterAt, off int64, uuid [16]byte, csum bool) error {
	sb := make([]byte, superblockSize)
	if _, err := f.ReadAt(sb, off); err != nil {
		return err
	}
	if binary.LittleEndian.Uint16(sb[offMagic:]) != magic {
		return errors.New("bad magic")
	}
	if csum && binary.LittleEndian.Uint32(sb[offChecksum:]) != checksum(sb) {
		return errors.New("bad checksum")
	}
	copy(sb[offUUID:], uuid[:])
	if csum {
		binary.LittleEndian.PutUint32(sb[offChecksum:], checksum(sb))
	}
	_, err := f.WriteAt(sb, off)
	return err
}

// checksum is crc32c(~0, sb) without the final inversion, as ext4 stores it
func checksum(sb []byte) uint32 {
	return ^crc32.Checksum(sb[:offChecksum], castagnoli)
}

// hasSuper reports whether group g holds a superblock copy, like
// ext4_bg_has_super in fs/ext4/balloc.c
func hasSuper(g int64, compat, roCompat uint32, backups [2]int64) bool {
	switch {
	case g == 0:
		return true
	case compat&compatSparseSuper2 != 0:
		return g == backups[0] || g == backups[1]
	case g <= 1 || roCompat&roCompatSparseSuper == 0:
		return true
	case g%2 == 0:
		return false
	}
	return isPower(g, 3) || isPower(g, 5) || isPower(g, 7)
}

func isPower(n, base int64) bool {
	for n > 1 && n%base == 0 {
		n /= base
	}
	return n == 1
}
