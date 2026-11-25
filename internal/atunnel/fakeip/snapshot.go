// Copyright 2026 The Agent Substrate Authors
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

package fakeip

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"

	"go.ate.dev/substrate/internal/egresspolicy"
)

// FileName is the snapshot file a table is saved in.
const FileName = "dns-fake-addresses.json"

// fileVersion is the format of the snapshot file. A worker starts with an
// empty table when it finds a version it does not know.
const fileVersion = 1

// savedTable is the content of the snapshot file.
type savedTable struct {
	Version int          `json:"version"`
	Pool    netip.Prefix `json:"pool"`
	Next    uint32       `json:"next"`
	Left    uint32       `json:"left"`
	// Names go from the one used longest ago to the one used last.
	Names []savedName `json:"names"`
}

type savedName struct {
	Name    string     `json:"name"`
	Address netip.Addr `json:"address"`
}

// Save writes the table into dir and returns the name of the file. A nil
// table, or one that never gave out an address, writes nothing and returns "".
func (t *Table) Save(dir string) (string, error) {
	if t == nil {
		return "", nil
	}
	t.mu.Lock()
	content := savedTable{
		Version: fileVersion,
		Pool:    pool,
		Next:    t.next,
		Left:    t.left,
		Names:   make([]savedName, 0, t.recent.Len()),
	}
	for element := t.recent.Back(); element != nil; element = element.Prev() {
		e := element.Value.(entry)
		content.Names = append(content.Names, savedName{Name: e.name, Address: addressAt(e.index)})
	}
	t.mu.Unlock()
	if len(content.Names) == 0 {
		return "", nil
	}

	data, err := json.Marshal(content)
	if err != nil {
		return "", fmt.Errorf("fakeip: encoding the table: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, FileName), data, 0o600); err != nil {
		return "", fmt.Errorf("fakeip: saving the table: %w", err)
	}
	return FileName, nil
}

// Load returns the table saved in dir, logging to log, or nil if dir holds
// none.
func Load(dir string, log *slog.Logger) (*Table, error) {
	data, err := os.ReadFile(filepath.Join(dir, FileName))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("fakeip: reading the saved table: %w", err)
	}
	var content savedTable
	if err := json.Unmarshal(data, &content); err != nil {
		return nil, fmt.Errorf("fakeip: decoding the saved table: %w", err)
	}
	if content.Version != fileVersion {
		return nil, fmt.Errorf("fakeip: the saved table has version %d, this worker reads version %d", content.Version, fileVersion)
	}
	if content.Pool != pool {
		return nil, fmt.Errorf("fakeip: the saved table uses addresses from %s, this worker uses %s", content.Pool, pool)
	}
	if content.Next >= poolSize || content.Left > poolSize {
		return nil, fmt.Errorf("fakeip: the saved table's next position %d with %d left is outside the pool", content.Next, content.Left)
	}

	t := newTable(log, capacity, content.Next, content.Left)
	// A table saved with a larger capacity keeps its most recently used names.
	names := content.Names[max(0, len(content.Names)-t.capacity):]
	for _, saved := range names {
		// The name ends up in a CONNECT request, so it has to be a hostname.
		if name, ok := egresspolicy.NormalizeHostname(saved.Name); !ok || name != saved.Name {
			return nil, fmt.Errorf("fakeip: the saved table holds %q, which is not a hostname", saved.Name)
		}
		if !Contains(saved.Address) || !usable(indexOf(saved.Address)) {
			return nil, fmt.Errorf("fakeip: the saved table holds %s, which is not a fake address", saved.Address)
		}
		index := indexOf(saved.Address)
		// The table gives out the next content.Left positions from
		// content.Next on, so none of them can belong to a name already.
		if (index+poolSize-content.Next)%poolSize < content.Left {
			return nil, fmt.Errorf("fakeip: the saved table holds %s, which it has not given out yet", saved.Address)
		}
		if _, ok := t.byName[saved.Name]; ok {
			return nil, fmt.Errorf("fakeip: the saved table holds %q twice", saved.Name)
		}
		if _, ok := t.byIndex[index]; ok {
			return nil, fmt.Errorf("fakeip: the saved table holds %s twice", saved.Address)
		}
		t.add(saved.Name, index)
	}
	return t, nil
}
