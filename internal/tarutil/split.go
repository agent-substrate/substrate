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

package tarutil

import (
	"archive/tar"
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Split rewrites the archive at tarPath, whose entries are named <dir>/...,
// into one archive per top-level directory: the entries under <dir> go to
// dsts[<dir>], named relative to <dir>, and <dir>'s own entry becomes the "./"
// root entry CreateWithRoot writes, so Extract applies its metadata to the
// destination directory. A key of dsts with no entries in tarPath gets no
// archive. Entry contents, ownership, modes, times and xattrs are copied
// unchanged; nothing is extracted, so the archives Split writes go through
// Extract's checks like any other.
//
// Split fails on an archive it cannot read, an entry outside every key of
// dsts (a top-level non-directory included), and a hardlink into another
// top-level directory, which the split archives cannot represent. It is all or
// nothing: each archive is written beside its destination and renamed into
// place only after tarPath has been read to the end; tarPath is left as is.
func Split(tarPath string, dsts map[string]string) (retErr error) {
	f, err := os.Open(tarPath)
	if err != nil {
		return fmt.Errorf("opening tar %q: %w", tarPath, err)
	}
	defer f.Close()

	outs := map[string]*splitOut{}
	defer func() {
		for _, out := range outs {
			out.abort()
		}
	}()

	tr := tar.NewReader(bufio.NewReaderSize(f, streamBufSize))
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading tar %q: %w", tarPath, err)
		}
		name, skip, err := cleanTarName(hdr.Name)
		if err != nil {
			return fmt.Errorf("invalid entry in tar %q: %w", tarPath, err)
		}
		if skip {
			continue
		}
		top, rel, _ := strings.Cut(filepath.ToSlash(name), "/")
		dst, ok := dsts[top]
		if !ok {
			return fmt.Errorf("entry %q in tar %q is outside every directory to split into", hdr.Name, tarPath)
		}
		if rel == "" && hdr.Typeflag != tar.TypeDir {
			return fmt.Errorf("entry %q in tar %q is not a directory", hdr.Name, tarPath)
		}
		out, err := splitHeader(hdr, top, rel)
		if err != nil {
			return fmt.Errorf("entry %q in tar %q: %w", hdr.Name, tarPath, err)
		}
		w, ok := outs[top]
		if !ok {
			if w, err = newSplitOut(dst); err != nil {
				return err
			}
			outs[top] = w
		}
		if err := w.tw.WriteHeader(out); err != nil {
			return fmt.Errorf("writing tar header for %q to %q: %w", out.Name, dst, err)
		}
		if _, err := copyPooled(w.tw, tr); err != nil {
			return fmt.Errorf("copying %q from tar %q: %w", hdr.Name, tarPath, err)
		}
	}

	for _, out := range outs {
		if err := out.finish(); err != nil {
			return err
		}
	}
	for top, out := range outs {
		if err := os.Rename(out.tmp, out.dst); err != nil {
			return fmt.Errorf("renaming %q to %q: %w", out.tmp, out.dst, err)
		}
		delete(outs, top)
	}
	return nil
}

// splitHeader returns hdr renamed to rel, its path below top, for the archive
// of top alone. The PAX records keep only the xattrs: the reader also records
// the original path and link path there, which would override the new names.
func splitHeader(hdr *tar.Header, top, rel string) (*tar.Header, error) {
	out := *hdr
	out.Format = tar.FormatUnknown
	out.PAXRecords = nil
	for k, v := range hdr.PAXRecords {
		if strings.HasPrefix(k, "SCHILY.xattr.") {
			if out.PAXRecords == nil {
				out.PAXRecords = map[string]string{}
			}
			out.PAXRecords[k] = v
		}
	}
	switch {
	case rel == "":
		out.Name = "./"
	case hdr.Typeflag == tar.TypeDir:
		out.Name = rel + "/"
	default:
		out.Name = rel
	}
	if hdr.Typeflag == tar.TypeLink {
		target, skip, err := cleanTarName(hdr.Linkname)
		if err != nil || skip {
			return nil, fmt.Errorf("invalid hardlink target %q", hdr.Linkname)
		}
		targetTop, targetRel, _ := strings.Cut(filepath.ToSlash(target), "/")
		if targetTop != top || targetRel == "" {
			return nil, fmt.Errorf("hardlink target %q is outside %q", hdr.Linkname, top)
		}
		out.Linkname = targetRel
	}
	return &out, nil
}

// splitOut is one archive Split writes, at tmp until it is complete.
type splitOut struct {
	dst, tmp string
	f        *os.File
	bw       *bufio.Writer
	tw       *tar.Writer
}

func newSplitOut(dst string) (*splitOut, error) {
	tmp := dst + ".split"
	f, err := os.Create(tmp)
	if err != nil {
		return nil, fmt.Errorf("creating tar %q: %w", tmp, err)
	}
	bw := bufio.NewWriterSize(f, streamBufSize)
	return &splitOut{dst: dst, tmp: tmp, f: f, bw: bw, tw: tar.NewWriter(bw)}, nil
}

// finish closes the archive and syncs it to disk, as create does.
func (o *splitOut) finish() error {
	if err := o.tw.Close(); err != nil {
		return fmt.Errorf("closing tar %q: %w", o.tmp, err)
	}
	if err := o.bw.Flush(); err != nil {
		return fmt.Errorf("flushing tar %q: %w", o.tmp, err)
	}
	if err := o.f.Sync(); err != nil {
		return fmt.Errorf("syncing tar %q: %w", o.tmp, err)
	}
	return o.f.Close()
}

// abort removes an archive Split did not rename into place.
func (o *splitOut) abort() {
	o.f.Close()
	os.Remove(o.tmp)
}
