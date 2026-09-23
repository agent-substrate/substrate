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

package ateomnet

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// SandboxResolvConf replaces nameservers with the sandbox gateway while
// preserving the pod's search domains and options for Kubernetes DNS.
func SandboxResolvConf(podResolvConf []byte) []byte {
	var out strings.Builder
	out.WriteString("nameserver " + ActorVethGateway + "\n")
	for line := range strings.SplitSeq(string(podResolvConf), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "nameserver") {
			continue
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		out.WriteString(line + "\n")
	}
	return []byte(out.String())
}

// WriteRootfsResolvConf installs content at /etc/resolv.conf inside rootfs.
//
// os.Root confines path traversal; unlinking prevents writes through existing links.
func WriteRootfsResolvConf(rootfs string, content []byte) error {
	if len(content) == 0 {
		return fmt.Errorf("actornet: refusing to write an empty resolv.conf")
	}
	root, err := os.OpenRoot(rootfs)
	if err != nil {
		return fmt.Errorf("opening rootfs %q: %w", rootfs, err)
	}
	defer root.Close()
	if err := root.Mkdir("etc", 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("creating %q: %w", filepath.Join(rootfs, "etc"), err)
	}
	if err := root.Remove("etc/resolv.conf"); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("removing existing resolv.conf: %w", err)
	}
	f, err := root.OpenFile("etc/resolv.conf", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("creating resolv.conf: %w", err)
	}
	_, err = f.Write(content)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("writing resolv.conf: %w", err)
	}
	return nil
}
