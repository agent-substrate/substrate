//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"archive/tar"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type tarEntry struct {
	name     string
	typeflag byte
	mode     int64
	body     string
	linkname string
}

func defaultMode(typeflag byte) int64 {
	switch typeflag {
	case tar.TypeDir:
		return 0o755
	case tar.TypeSymlink:
		return 0o777
	default:
		return 0o644
	}
}

func buildTar(t *testing.T, entries []tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		mode := e.mode
		if mode == 0 {
			mode = defaultMode(e.typeflag)
		}
		hdr := &tar.Header{
			Name:     e.name,
			Typeflag: e.typeflag,
			Mode:     mode,
			Size:     int64(len(e.body)),
			Linkname: e.linkname,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("tar.WriteHeader(%+v): %v", hdr, err)
		}
		if e.body != "" {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatalf("tar.Write(%q): %v", e.name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar.Close: %v", err)
	}
	return buf.Bytes()
}

func runUntar(t *testing.T, entries []tarEntry) (string, error) {
	t.Helper()
	dir := t.TempDir()
	return dir, untar(context.Background(), bytes.NewReader(buildTar(t, entries)), dir)
}

func TestValidateTarName(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantClean string
		wantSkip  bool
		wantErr   bool
	}{
		{name: "regular file", input: "etc/passwd", wantClean: "etc/passwd"},
		{name: "current dir", input: ".", wantSkip: true},
		{name: "empty", input: "", wantSkip: true},
		{name: "trailing slash", input: "etc/", wantClean: "etc"},
		{name: "absolute path", input: "/etc/passwd", wantClean: "etc/passwd"},
		{name: "double slash absolute", input: "//etc/passwd", wantClean: "etc/passwd"},
		{name: "parent escape", input: "../etc/passwd", wantErr: true},
		{name: "parent only", input: "..", wantErr: true},
		{name: "embedded escape", input: "a/../../escape", wantErr: true},
		{name: "ok with dot segments", input: "./a/./b", wantClean: "a/b"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotClean, gotSkip, err := validateTarName(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateTarName(%q) err = %v, wantErr %v", tc.input, err, tc.wantErr)
			}
			if err != nil {
				return
			}
			if gotSkip != tc.wantSkip {
				t.Errorf("skip = %v, want %v", gotSkip, tc.wantSkip)
			}
			if gotClean != tc.wantClean {
				t.Errorf("clean = %q, want %q", gotClean, tc.wantClean)
			}
		})
	}
}

func TestUntar_HappyPath(t *testing.T) {
	entries := []tarEntry{
		{name: ".", typeflag: tar.TypeDir},
		{name: "etc/", typeflag: tar.TypeDir},
		{name: "etc/hostname", typeflag: tar.TypeReg, body: "demo\n"},
		{name: "bin/", typeflag: tar.TypeDir},
		{name: "bin/sh", typeflag: tar.TypeReg, mode: 0o755, body: "#!/sh\n"},
		{name: "bin/bash", typeflag: tar.TypeLink, linkname: "bin/sh"},
		{name: "etc/host-link", typeflag: tar.TypeSymlink, linkname: "hostname"},
	}
	dir, err := runUntar(t, entries)
	if err != nil {
		t.Fatalf("untar: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(dir, "etc/hostname")); err != nil {
		t.Errorf("read etc/hostname: %v", err)
	} else if string(got) != "demo\n" {
		t.Errorf("etc/hostname = %q, want %q", got, "demo\n")
	}

	if target, err := os.Readlink(filepath.Join(dir, "etc/host-link")); err != nil {
		t.Errorf("readlink etc/host-link: %v", err)
	} else if target != "hostname" {
		t.Errorf("symlink target = %q, want %q", target, "hostname")
	}

	srcInfo, err := os.Stat(filepath.Join(dir, "bin/sh"))
	if err != nil {
		t.Fatalf("stat bin/sh: %v", err)
	}
	dstInfo, err := os.Stat(filepath.Join(dir, "bin/bash"))
	if err != nil {
		t.Fatalf("stat bin/bash: %v", err)
	}
	if !os.SameFile(srcInfo, dstInfo) {
		t.Errorf("bin/bash is not a hardlink to bin/sh")
	}
}

func TestUntar_LaterEntryWins(t *testing.T) {
	t.Run("dir then symlink", func(t *testing.T) {
		entries := []tarEntry{
			{name: "var/", typeflag: tar.TypeDir},
			{name: "var/run/", typeflag: tar.TypeDir},
			{name: "run/", typeflag: tar.TypeDir},
			{name: "run/sock", typeflag: tar.TypeReg, body: "sock"},
			{name: "var/run", typeflag: tar.TypeSymlink, linkname: "../run"},
		}
		dir, err := runUntar(t, entries)
		if err != nil {
			t.Fatalf("untar: %v", err)
		}
		fi, err := os.Lstat(filepath.Join(dir, "var/run"))
		if err != nil {
			t.Fatalf("lstat var/run: %v", err)
		}
		if fi.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("var/run not a symlink, mode = %v", fi.Mode())
		}
		if got, _ := os.Readlink(filepath.Join(dir, "var/run")); got != "../run" {
			t.Errorf("symlink target = %q, want %q", got, "../run")
		}
	})

	t.Run("file overwrite", func(t *testing.T) {
		entries := []tarEntry{
			{name: "etc/", typeflag: tar.TypeDir},
			{name: "etc/conf", typeflag: tar.TypeReg, body: "v1"},
			{name: "etc/conf", typeflag: tar.TypeReg, body: "v2"},
		}
		dir, err := runUntar(t, entries)
		if err != nil {
			t.Fatalf("untar: %v", err)
		}
		if got, _ := os.ReadFile(filepath.Join(dir, "etc/conf")); string(got) != "v2" {
			t.Errorf("etc/conf = %q, want %q", got, "v2")
		}
	})

	t.Run("symlink retargeted", func(t *testing.T) {
		entries := []tarEntry{
			{name: "etc/", typeflag: tar.TypeDir},
			{name: "etc/x", typeflag: tar.TypeReg, body: "x"},
			{name: "etc/y", typeflag: tar.TypeReg, body: "y"},
			{name: "etc/link", typeflag: tar.TypeSymlink, linkname: "x"},
			{name: "etc/link", typeflag: tar.TypeSymlink, linkname: "y"},
		}
		dir, err := runUntar(t, entries)
		if err != nil {
			t.Fatalf("untar: %v", err)
		}
		if got, _ := os.Readlink(filepath.Join(dir, "etc/link")); got != "y" {
			t.Errorf("symlink target = %q, want %q", got, "y")
		}
	})

	t.Run("repeated dir entry tolerated", func(t *testing.T) {
		entries := []tarEntry{
			{name: "etc/", typeflag: tar.TypeDir},
			{name: "etc/", typeflag: tar.TypeDir},
		}
		if _, err := runUntar(t, entries); err != nil {
			t.Errorf("untar: %v", err)
		}
	})

	t.Run("identical symlink redeclaration is a no-op", func(t *testing.T) {
		entries := []tarEntry{
			{name: "etc/", typeflag: tar.TypeDir},
			{name: "etc/x", typeflag: tar.TypeReg, body: "x"},
			{name: "etc/link", typeflag: tar.TypeSymlink, linkname: "x"},
			{name: "etc/link", typeflag: tar.TypeSymlink, linkname: "x"},
		}
		dir, err := runUntar(t, entries)
		if err != nil {
			t.Fatalf("untar: %v", err)
		}
		if got, _ := os.Readlink(filepath.Join(dir, "etc/link")); got != "x" {
			t.Errorf("symlink target = %q, want %q", got, "x")
		}
	})
}

func TestUntar_PathTraversal(t *testing.T) {
	tests := []struct {
		name  string
		entry tarEntry
	}{
		{name: "parent prefix", entry: tarEntry{name: "../escape", typeflag: tar.TypeReg, body: "x"}},
		{name: "embedded parent", entry: tarEntry{name: "a/b/../../../escape", typeflag: tar.TypeReg, body: "x"}},
		{name: "parent only", entry: tarEntry{name: "..", typeflag: tar.TypeReg, body: "x"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runUntar(t, []tarEntry{tc.entry})
			if err == nil {
				t.Fatalf("untar(%q) succeeded, want error", tc.entry.name)
			}
			if !strings.Contains(err.Error(), "invalid tar entry") {
				t.Errorf("error = %q, want it to mention 'invalid tar entry'", err.Error())
			}
		})
	}
}

func TestUntar_SymlinkEscape(t *testing.T) {
	// CVE-2024-24579 / CVE-2020-27833 pattern: a tar declares a symlink
	// pointing outside the rootfs, then a later entry writes through it.
	parent := t.TempDir()
	rootfsDir := filepath.Join(parent, "rootfs")
	if err := os.Mkdir(rootfsDir, 0o755); err != nil {
		t.Fatalf("mkdir rootfs: %v", err)
	}
	hostDir := filepath.Join(parent, "host")
	if err := os.Mkdir(hostDir, 0o755); err != nil {
		t.Fatalf("mkdir host: %v", err)
	}
	hostFile := filepath.Join(hostDir, "passwd")
	if err := os.WriteFile(hostFile, []byte("original"), 0o644); err != nil {
		t.Fatalf("write host file: %v", err)
	}

	entries := []tarEntry{
		{name: "etc", typeflag: tar.TypeSymlink, linkname: hostDir},
		{name: "etc/passwd", typeflag: tar.TypeReg, body: "OWNED"},
	}
	if err := untar(context.Background(), bytes.NewReader(buildTar(t, entries)), rootfsDir); err == nil {
		t.Fatalf("untar succeeded; expected escape via symlink to be refused")
	}

	got, err := os.ReadFile(hostFile)
	if err != nil {
		t.Fatalf("read host file: %v", err)
	}
	if string(got) != "original" {
		t.Errorf("host file modified to %q -- symlink escape was NOT prevented", got)
	}
}

func TestUntar_HardlinkEscape(t *testing.T) {
	tests := []struct {
		name  string
		entry tarEntry
	}{
		{name: "parent target", entry: tarEntry{name: "etc/passwd", typeflag: tar.TypeLink, linkname: "../host/passwd"}},
		{name: "embedded escape target", entry: tarEntry{name: "etc/passwd", typeflag: tar.TypeLink, linkname: "a/../../host"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runUntar(t, []tarEntry{tc.entry})
			if err == nil {
				t.Fatalf("untar succeeded, want hardlink escape refused")
			}
			if !strings.Contains(err.Error(), "invalid hardlink target") {
				t.Errorf("error = %q, want it to mention 'invalid hardlink target'", err.Error())
			}
		})
	}
}

func TestUntar_RejectSpecialFiles(t *testing.T) {
	tests := []struct {
		name     string
		typeflag byte
	}{
		{name: "char device", typeflag: tar.TypeChar},
		{name: "block device", typeflag: tar.TypeBlock},
		{name: "fifo", typeflag: tar.TypeFifo},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runUntar(t, []tarEntry{{name: "weird", typeflag: tc.typeflag}})
			if err == nil {
				t.Fatalf("untar succeeded, want unhandled-typeflag error")
			}
			if !strings.Contains(err.Error(), "unhandled tar entry typeflag") {
				t.Errorf("error = %q, want it to mention 'unhandled tar entry typeflag'", err.Error())
			}
		})
	}
}

func TestUntar_TruncatedArchive(t *testing.T) {
	full := buildTar(t, []tarEntry{
		{name: "ok", typeflag: tar.TypeReg, body: "hello"},
	})
	if len(full) < 64 {
		t.Fatalf("buildTar produced suspiciously small output: %d bytes", len(full))
	}
	truncated := full[:len(full)-64]

	dir := t.TempDir()
	err := untar(context.Background(), bytes.NewReader(truncated), dir)
	if err == nil {
		t.Fatalf("untar on truncated archive succeeded; want error")
	}
	if !strings.Contains(err.Error(), "in tarReader.Next") &&
		!strings.Contains(err.Error(), "unexpected EOF") {
		t.Errorf("error = %v, want it to surface the underlying tar/copy error", err)
	}
}
