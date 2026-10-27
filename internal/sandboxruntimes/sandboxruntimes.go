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

// Package sandboxruntimes reads and writes a worker's sandbox runtimes file,
// which atelet keeps in the worker's directory (nodepath.WorkerPath) as
// nodepath.SandboxRuntimesFileName.
package sandboxruntimes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// File is the content of a worker's sandbox runtimes file: the sandbox
// classes the worker can run and, for each, the compatibility attributes
// atelet computed for the runtimes of that class. The worker registers it as
// RegisterWorkerRequest's sandbox_classes.
type File struct {
	// SandboxClasses has one entry per class the worker's pool declares:
	// one today.
	SandboxClasses []Class `json:"sandboxClasses"`
}

// Class is one sandbox class and the runtimes the worker runs it under.
type Class struct {
	// SandboxClass is e.g. "gvisor".
	SandboxClass string `json:"sandboxClass"`
	// DefaultRuntime is the runtime new and cold-booted actors of this class
	// start under, from the default version of the pool's SandboxConfig.
	DefaultRuntime Runtime `json:"defaultRuntime"`
	// RestorableRuntimes are the other runtimes the worker can restore
	// snapshots of this class from, from the config's other Enabled versions
	// in spec order. None repeats another runtime or the default.
	RestorableRuntimes []Runtime `json:"restorableRuntimes,omitempty"`
}

// Runtime is one runtime of a class, as RegisterWorkerRequest's
// WorkerSandboxRuntime.
type Runtime struct {
	// Version is the compatibility attributes atelet computed for the
	// runtime, which the worker registers under the current schema version.
	Version Attributes `json:"version"`
}

// Attributes maps a compatibility attribute key, e.g. "sandbox_bundle_hash",
// to its value.
type Attributes map[string]string

// tempPrefix starts the name of every temporary file Write renames into
// place. It is dot-prefixed so a directory listing does not show one.
const tempPrefix = ".sandbox-runtimes-"

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
