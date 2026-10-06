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

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
)

const hypervisorFile = "hypervisor"

func writeSnapshotHypervisor(dir string, backend ch.Hypervisor) error {
	if backend != ch.KVM && backend != ch.MSHV {
		return fmt.Errorf("unknown snapshot hypervisor %q", backend)
	}
	return os.WriteFile(filepath.Join(dir, hypervisorFile), []byte(backend), 0o600)
}

func validateSnapshotHypervisor(dir string, backend ch.Hypervisor) error {
	b, err := os.ReadFile(filepath.Join(dir, hypervisorFile))
	if err != nil {
		return fmt.Errorf("read snapshot hypervisor: %w", err)
	}
	source := ch.Hypervisor(b)
	if source != ch.KVM && source != ch.MSHV {
		return fmt.Errorf("unknown snapshot hypervisor %q", source)
	}
	if source != backend {
		return fmt.Errorf("snapshot hypervisor %q is incompatible with worker hypervisor %q", source, backend)
	}
	return nil
}
