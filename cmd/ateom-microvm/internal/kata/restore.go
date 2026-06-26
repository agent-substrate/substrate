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

package kata

import (
	"path/filepath"
)

// Per-sandbox runtime paths. The CH snapshot's config.json references the
// per-sandbox hybrid-vsock socket as an absolute path, so restore must recreate
// it (or rewrite config.json) for the sandbox id.

// VMDir is the per-sandbox runtime dir (holds the cloud-hypervisor API socket and
// the hybrid-vsock socket).
func VMDir(id string) string { return filepath.Join(vcVMDir, id) }

// VsockSocketPath is the hybrid-vsock socket the CH snapshot's vsock device
// references; CH recreates the listener here on restore.
func VsockSocketPath(id string) string { return filepath.Join(VMDir(id), "clh.sock") }
