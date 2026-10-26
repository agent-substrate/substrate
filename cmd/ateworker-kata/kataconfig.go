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
	"fmt"
	"os"

	"github.com/pelletier/go-toml/v2"
)

// kataConfig is what the worker reads from a Kata configuration file: the
// hypervisor it selects, and whether that hypervisor can share a host
// directory with the guest. The shim reads the rest.
type kataConfig struct {
	hypervisor string
	sharedFS   string
}

// readKataConfig reads the hypervisor section of the Kata configuration file
// at path.
func readKataConfig(path string) (kataConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return kataConfig{}, err
	}
	var doc struct {
		Hypervisor map[string]struct {
			SharedFS string `toml:"shared_fs"`
		} `toml:"hypervisor"`
	}
	if err := toml.Unmarshal(data, &doc); err != nil {
		return kataConfig{}, fmt.Errorf("decoding %s: %w", path, err)
	}
	if len(doc.Hypervisor) != 1 {
		return kataConfig{}, fmt.Errorf("%s has %d [hypervisor.*] sections, want exactly one", path, len(doc.Hypervisor))
	}
	var c kataConfig
	for name, h := range doc.Hypervisor {
		c = kataConfig{hypervisor: name, sharedFS: h.SharedFS}
	}
	return c, nil
}

// sharesHostDirs reports whether the guest sees host directories. Firecracker
// has no shared filesystem, and a configuration can also turn it off.
func (c kataConfig) sharesHostDirs() bool {
	return c.hypervisor != "firecracker" && c.sharedFS != "none"
}
