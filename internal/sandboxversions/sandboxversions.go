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

// Package sandboxversions reads and writes a worker's sandbox versions file,
// which atelet keeps in the worker's directory (nodepath.AteomPath) as
// nodepath.SandboxVersionsFileName.
package sandboxversions

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// File is the content of a worker's sandbox versions file: the enabled
// versions of the SandboxConfig the worker's pool uses, each with its assets
// already fetched to the node.
type File struct {
	// SandboxClass is the sandbox class of the worker's pool.
	SandboxClass string `json:"sandboxClass,omitempty"`
	// SandboxConfig names the SandboxConfig the versions come from.
	SandboxConfig string `json:"sandboxConfig,omitempty"`
	// Versions lists the config's default version first, then its other
	// Enabled versions in spec order. Versions with no assets for this node's
	// architecture are left out.
	Versions []Version `json:"versions,omitempty"`
}

// Version is one version of a SandboxConfig, projected onto the node's
// architecture and fetched to the node.
type Version struct {
	// Name is the SandboxConfig version name, e.g. "gvisor-4".
	Name string `json:"name,omitempty"`
	// IsDefault is whether this is the config's defaultVersion, which new and
	// cold-booted actors start under.
	IsDefault bool `json:"isDefault,omitempty"`
	// PauseImage is the root sandbox container image. Only gVisor has one.
	PauseImage string `json:"pauseImage,omitempty"`
	// AssetPaths maps asset name to local path on the node, as
	// RunWorkloadRequest's runsc_path and runtime_asset_paths.
	AssetPaths map[string]string `json:"assetPaths,omitempty"`
	// AssetDigests maps asset name to the asset's sha256.
	AssetDigests map[string]string `json:"assetDigests,omitempty"`
}

// tempPrefix starts the name of every temporary file Write renames into
// place. It is dot-prefixed so a directory listing does not show one.
const tempPrefix = ".sandbox-versions-"

// Write atomically replaces the file at path with v, unless it already holds
// the same content. It reports whether it wrote.
func Write(path string, v *File) (bool, error) {
	// encoding/json sorts map keys, so equal content encodes to equal bytes.
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return false, fmt.Errorf("while encoding %s: %w", path, err)
	}
	if cur, err := os.ReadFile(path); err == nil && bytes.Equal(cur, data) {
		return false, nil
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, tempPrefix+"*")
	if err != nil {
		return false, err
	}
	tmp := f.Name()
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		os.Remove(tmp)
		return false, fmt.Errorf("while writing %s: %w", path, err)
	}
	return true, nil
}

// Read returns the content of the file at path. A missing file is an error
// wrapping fs.ErrNotExist.
func Read(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	v := &File{}
	if err := dec.Decode(v); err != nil {
		return nil, fmt.Errorf("while decoding %s: %w", path, err)
	}
	return v, nil
}

// IsTemp reports whether name is one of Write's temporary files.
func IsTemp(name string) bool {
	return strings.HasPrefix(filepath.Base(name), tempPrefix)
}
