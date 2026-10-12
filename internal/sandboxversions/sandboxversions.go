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

// Package sandboxversions reads and writes a worker's sandbox versions file
// (nodepath.AteomSandboxVersionsFile): atelet writes it, the ateom reads it.
package sandboxversions

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// tempPrefix starts the name of every temporary file Write renames into
// place. It is dot-prefixed so a directory listing does not show one.
const tempPrefix = ".sandbox-versions-"

// Write atomically replaces the file at path with v, unless it already holds
// an equal message. It reports whether it wrote.
func Write(path string, v *ateletpb.WorkerSandboxVersions) (bool, error) {
	if cur, err := Read(path); err == nil && proto.Equal(cur, v) {
		return false, nil
	}
	// protojson output is for people reading the file on the node; the
	// reader compares parsed messages, never bytes.
	data, err := protojson.MarshalOptions{Multiline: true}.Marshal(v)
	if err != nil {
		return false, fmt.Errorf("while encoding %s: %w", path, err)
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

// Read returns the message in the file at path. A missing file is an error
// wrapping fs.ErrNotExist.
func Read(path string) (*ateletpb.WorkerSandboxVersions, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	v := &ateletpb.WorkerSandboxVersions{}
	if err := protojson.Unmarshal(data, v); err != nil {
		return nil, fmt.Errorf("while decoding %s: %w", path, err)
	}
	return v, nil
}

// IsTemp reports whether name is one of Write's temporary files, which a
// watcher of the directory ignores.
func IsTemp(name string) bool {
	return strings.HasPrefix(filepath.Base(name), tempPrefix)
}
