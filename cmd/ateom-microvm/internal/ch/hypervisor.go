// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ch

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
)

// Hypervisor identifies the backend selected by Cloud Hypervisor.
type Hypervisor string

const (
	KVM  Hypervisor = "kvm"
	MSHV Hypervisor = "mshv"
)

// DetectHypervisor identifies the backend available to this worker using the
// features reported by the running VMM.
func (i VMMInfo) DetectHypervisor() (Hypervisor, error) {
	return detectHypervisor(i.Features, "/dev")
}

// detectHypervisor follows Cloud Hypervisor's compiled-feature and device-path
// selection order. Compiled features alone do not identify the active backend.
func detectHypervisor(features []string, devRoot string) (Hypervisor, error) {
	for _, backend := range []Hypervisor{KVM, MSHV} {
		if !slices.Contains(features, string(backend)) {
			continue
		}
		path := filepath.Join(devRoot, string(backend))
		info, err := os.Stat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("inspect hypervisor device %s: %w", path, err)
		}
		if info.Mode()&os.ModeCharDevice == 0 {
			return "", fmt.Errorf("hypervisor device %s is not a character device", path)
		}
		return backend, nil
	}
	return "", fmt.Errorf("no supported hypervisor device for VMM features %v", features)
}
