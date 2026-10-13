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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.ate.dev/substrate/internal/apierror"
)

// vmmFeaturesTimeout bounds `cloud-hypervisor --version -v`.
const vmmFeaturesTimeout = 5 * time.Second

// maxVMMBuilds bounds how many Cloud Hypervisor builds vmmBackends
// remembers; past it the cache starts over.
const maxVMMBuilds = 64

// vmmBackends remembers the compiled features of each Cloud Hypervisor build.
// Each actor runs the binary from its own sandbox assets dir, hard-linked from
// atelet's content-addressed asset cache, so the path names the actor rather
// than the build, and a later restore of the same actor may stage a different
// build there. The file's identity (vmmBuild) names the build instead: actors
// running the same build share its inode. The zero value is ready to use.
type vmmBackends struct {
	// lookPath resolves a bare binary name; nil means exec.LookPath.
	lookPath func(string) (string, error)

	mu       sync.Mutex
	features map[vmmBuild][]string
}

// vmmBuild identifies a Cloud Hypervisor binary by its file rather than its
// path. Size and modification time guard against an inode number reused
// after atelet evicts a build from its cache.
type vmmBuild struct {
	dev, ino uint64
	size     int64
	mtimeNs  int64
}

// resolve returns the file bin runs, resolving a bare name through PATH as
// exec does, and that file's identity.
func (v *vmmBackends) resolve(bin string) (string, vmmBuild, error) {
	lookPath := v.lookPath
	if lookPath == nil {
		lookPath = exec.LookPath
	}
	path, err := lookPath(bin)
	if err != nil {
		return "", vmmBuild{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", vmmBuild{}, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", vmmBuild{}, fmt.Errorf("no inode for %s", path)
	}
	return path, vmmBuild{dev: uint64(st.Dev), ino: st.Ino, size: info.Size(), mtimeNs: info.ModTime().UnixNano()}, nil
}

// check returns FailedPrecondition when the Cloud Hypervisor binary at bin was
// built without the worker's backend h.
//
// Cloud Hypervisor picks its backend when it starts and exits before serving
// its api-socket when it has none for this host, which ateworker would otherwise
// report as an api-socket timeout. That happens when the SandboxConfig the
// worker's pool resolves ships a build for the other backend. If the features
// cannot be read, check passes and the launch reports its own failure: this
// only names a failure that would happen anyway.
func (v *vmmBackends) check(ctx context.Context, bin string, h hypervisor) error {
	features, ok := v.lookup(ctx, bin)
	if !ok || slices.Contains(features, string(h)) {
		return nil
	}
	return apierror.FailedPrecondition("cloud-hypervisor %s was built without %s support (features %v); give this worker's pool a SandboxConfig whose cloud-hypervisor supports %s", bin, h, features, h)
}

func (v *vmmBackends) lookup(ctx context.Context, bin string) ([]string, bool) {
	path, build, err := v.resolve(bin)
	if err != nil {
		slog.WarnContext(ctx, "Could not identify cloud-hypervisor binary", slog.String("binary", bin), slog.Any("err", err))
		return nil, false
	}
	v.mu.Lock()
	features, ok := v.features[build]
	v.mu.Unlock()
	if ok {
		return features, true
	}

	ctx, cancel := context.WithTimeout(ctx, vmmFeaturesTimeout)
	defer cancel()
	// --version returns before Cloud Hypervisor opens a hypervisor device, and
	// -v adds the compiled feature list.
	// Run the file the cache key names, not bin, which exec would resolve
	// again.
	out, err := exec.CommandContext(ctx, path, "--version", "-v").Output()
	if err != nil {
		slog.WarnContext(ctx, "Could not read cloud-hypervisor features", slog.String("binary", bin), slog.Any("err", err))
		return nil, false
	}
	features, ok = parseVMMFeatures(out)
	if !ok {
		slog.WarnContext(ctx, "cloud-hypervisor --version -v listed no features", slog.String("binary", bin))
		return nil, false
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	if v.features == nil || len(v.features) >= maxVMMBuilds {
		v.features = map[vmmBuild][]string{}
	}
	v.features[build] = features
	return features, true
}

// parseVMMFeatures reads the `Enabled features: ["kvm", "mshv"]` line that
// `cloud-hypervisor --version -v` prints.
func parseVMMFeatures(out []byte) ([]string, bool) {
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		list, ok := strings.CutPrefix(strings.TrimSpace(sc.Text()), "Enabled features:")
		if !ok {
			continue
		}
		list = strings.TrimSpace(list)
		if !strings.HasPrefix(list, "[") || !strings.HasSuffix(list, "]") {
			return nil, false
		}
		features := []string{}
		for f := range strings.SplitSeq(list[1:len(list)-1], ",") {
			if f = strings.Trim(strings.TrimSpace(f), `"`); f != "" {
				features = append(features, f)
			}
		}
		return features, true
	}
	return nil, false
}
