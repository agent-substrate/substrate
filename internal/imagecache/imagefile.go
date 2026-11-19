// Copyright 2026 The Agent Substrate Authors
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

package imagecache

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"syscall"
)

// maxImageFileSize caps ReadFile: it reads small config files from untrusted images.
const maxImageFileSize = 1 << 20

// ReadFile returns the regular file at name, absolute or rootfs-relative, as
// the composed image would present it, without mounting: the top-most layer
// holding the file wins unless a layer above it whited out the path or an
// ancestor, made an ancestor opaque, or has a non-directory ancestor. A
// missing file wraps fs.ErrNotExist. Symlinks resolve within their own layer;
// one whose target lives only in a lower layer reads as absent there, and the
// lookup falls through to the layer below.
//
// Store layers record their whiteouts and opaque dirs beside the tree. Other
// lowerdirs mark whiteouts as overlayfs does. ReadFile sees their whiteout
// devices, but not their opaque dirs: reading trusted.* xattrs needs
// CAP_SYS_ADMIN, which atelet drops.
func (img *Image) ReadFile(name string) ([]byte, error) {
	rel, skip, err := validateTarName(name)
	if err != nil {
		return nil, fmt.Errorf("image file %q: %w", name, err)
	}
	if skip {
		return nil, fmt.Errorf("image file %q: %w", name, fs.ErrInvalid)
	}
	lowerDirs := img.LowerDirs()
	for i := len(lowerDirs) - 1; i >= 0; i-- {
		b, err := readLayerFile(lowerDirs[i], rel)
		switch {
		case err == nil:
			return b, nil
		case errors.Is(err, syscall.ENOTDIR), errors.Is(err, errWhiteout):
			// A non-directory ancestor or a whiteout here shadows the path in
			// every layer below.
			return nil, fmt.Errorf("image file %q: %w", name, fs.ErrNotExist)
		case !errors.Is(err, fs.ErrNotExist):
			return nil, fmt.Errorf("image file %q: %w", name, err)
		}
		if i >= len(img.LayerDirs) {
			continue
		}
		hidden, err := layerHides(img.LayerDirs[i], rel)
		if err != nil {
			return nil, fmt.Errorf("image file %q: %w", name, err)
		}
		if hidden {
			break
		}
	}
	return nil, fmt.Errorf("image file %q: %w", name, fs.ErrNotExist)
}

// errWhiteout reports an overlayfs whiteout device at the path.
var errWhiteout = errors.New("whited out")

// readLayerFile reads rel from one layer's lowerdir, confined to it.
func readLayerFile(lower, rel string) ([]byte, error) {
	root, err := os.OpenRoot(lower)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	if fi, err := root.Lstat(rel); err == nil && isWhiteout(fi) {
		return nil, errWhiteout
	}
	f, err := root.Open(rel)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxImageFileSize+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxImageFileSize {
		return nil, fmt.Errorf("larger than %d bytes", maxImageFileSize)
	}
	return b, nil
}

// isWhiteout reports whether fi is an overlayfs whiteout, a 0:0 char device.
func isWhiteout(fi fs.FileInfo) bool {
	if fi.Mode()&fs.ModeCharDevice == 0 {
		return false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && st.Rdev == 0
}

// layerHides reports whether the layer's recorded whiteouts or opaque dirs
// hide rel in every layer below it.
func layerHides(layerDir, rel string) (bool, error) {
	wh, err := readWhiteouts(layerDir)
	if err != nil {
		return false, err
	}
	for _, p := range wh.Whiteouts {
		q, skip, err := validateTarName(p)
		if err != nil {
			return false, err
		}
		if !skip && (q == rel || strings.HasPrefix(rel, q+"/")) {
			return true, nil
		}
	}
	for _, p := range wh.Opaques {
		q, skip, err := validateTarName(p)
		if err != nil {
			return false, err
		}
		if !skip && strings.HasPrefix(rel, q+"/") {
			return true, nil
		}
	}
	return false, nil
}
