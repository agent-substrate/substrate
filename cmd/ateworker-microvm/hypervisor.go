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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/sandboxruntimes"
)

// hypervisor is the host hypervisor backend Cloud Hypervisor drives. Its value
// is also the device name under /dev and the worker's hardware.AttrHypervisor
// attribute, so snapshots are only placed on workers with the same backend.
type hypervisor string

const (
	hypervisorKVM  hypervisor = hardware.HypervisorKVM
	hypervisorMSHV hypervisor = hardware.HypervisorMSHV

	// hypervisorFile records the backend of a MEMORY snapshot taken on a
	// backend other than KVM. KVM is the default backend, so its snapshots
	// carry no marker.
	hypervisorFile = "hypervisor"
)

// detectHypervisor returns the backend whose device the worker was granted.
// The atecontroller requests exactly one of ate.dev/kvm and ate.dev/mshv, and
// the device plugin mounts only the granted device, so the container sees
// exactly one. Anything else is a misconfigured pod, and guessing would risk
// stamping snapshots with the wrong backend, so it is an error.
func detectHypervisor(devRoot string) (hypervisor, error) {
	var found []hypervisor
	for _, h := range []hypervisor{hypervisorKVM, hypervisorMSHV} {
		path := filepath.Join(devRoot, string(h))
		info, err := os.Stat(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("while inspecting hypervisor device %s: %w", path, err)
		}
		if info.Mode()&os.ModeCharDevice == 0 {
			return "", fmt.Errorf("hypervisor device %s is not a character device", path)
		}
		found = append(found, h)
	}
	switch len(found) {
	case 1:
		return found[0], nil
	case 0:
		return "", fmt.Errorf("no hypervisor device in %s: the worker must be granted ate.dev/kvm or ate.dev/mshv", devRoot)
	default:
		return "", fmt.Errorf("both %s/kvm and %s/mshv are present: the worker must be granted exactly one hypervisor device", devRoot, devRoot)
	}
}

// runtimeAttributes are the attributes the worker adds to its reported
// runtime. The control plane stamps them on snapshots and places a snapshot
// only on workers reporting the same values, so guest memory never moves
// between backends.
func runtimeAttributes(h hypervisor) sandboxruntimes.Attributes {
	return sandboxruntimes.Attributes{hardware.AttrHypervisor: string(h)}
}

// writeSnapshotHypervisor records h in a MEMORY snapshot's checkpoint dir.
func writeSnapshotHypervisor(dir string, h hypervisor) error {
	path := filepath.Join(dir, hypervisorFile)
	if h == hypervisorKVM {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("while removing %s: %w", hypervisorFile, err)
		}
		return nil
	}
	if err := os.WriteFile(path, []byte(h), 0o600); err != nil {
		return fmt.Errorf("while writing %s: %w", hypervisorFile, err)
	}
	return nil
}

// snapshotHypervisor reads the backend a MEMORY snapshot in dir was taken on.
func snapshotHypervisor(dir string) (hypervisor, error) {
	data, err := os.ReadFile(filepath.Join(dir, hypervisorFile))
	if errors.Is(err, fs.ErrNotExist) {
		return hypervisorKVM, nil
	}
	if err != nil {
		return "", fmt.Errorf("while reading %s: %w", hypervisorFile, err)
	}
	switch h := hypervisor(strings.TrimSpace(string(data))); h {
	case hypervisorMSHV:
		return h, nil
	default:
		return "", fmt.Errorf("unsupported hypervisor %q in %s", h, hypervisorFile)
	}
}

// validateSnapshotHypervisor rejects restoring guest memory captured on a
// different backend: Cloud Hypervisor's vCPU and device state is backend
// specific. The control plane places a snapshot only on a worker reporting the
// same backend, so this guards placements that bypass that check. It is a
// precondition of the placement that no retry on this worker can fix, hence
// FailedPrecondition rather than Internal.
func validateSnapshotHypervisor(dir string, want hypervisor) error {
	got, err := snapshotHypervisor(dir)
	if err != nil {
		return apierror.FailedPrecondition("snapshot hypervisor metadata is invalid: %v", err)
	}
	if got != want {
		return apierror.FailedPrecondition("snapshot was taken on hypervisor %s and cannot be restored on a %s worker", got, want)
	}
	return nil
}
