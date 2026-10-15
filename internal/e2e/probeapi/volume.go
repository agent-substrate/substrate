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

// Package probeapi defines the HTTP contracts shared by the probe and its tests.
package probeapi

import (
	"fmt"
	"slices"
)

type VolumeObservation struct {
	Data      string `json:"data"`
	Error     string `json:"error,omitempty"`
	ErrorCode string `json:"error_code,omitempty"`
	NotExist  bool   `json:"not_exist,omitempty"`
}

type VolumeStateResponse struct {
	Challenge      string            `json:"challenge"`
	Reads          uint64            `json:"reads"`
	Cached         string            `json:"cached"`
	ByPath         VolumeObservation `json:"by_path"`
	Held           VolumeObservation `json:"held"`
	Mapped         VolumeObservation `json:"mapped"`
	Unlinked       VolumeObservation `json:"unlinked"`
	Negative       VolumeObservation `json:"negative"`
	Directory      []string          `json:"directory"`
	DirectoryError string            `json:"directory_error,omitempty"`
}

// ValidateRetainedState checks the disposable fixture across an own-snapshot
// restore. The microVM-only second-generation EIO exception must be explicit.
func ValidateRetainedState(obs VolumeStateResponse, challenge, value string, generation int, allowDeletedOpenEIO bool) error {
	if obs.Challenge != challenge || obs.Cached != value {
		return fmt.Errorf("application state mismatch: %+v", obs)
	}
	for name, got := range map[string]VolumeObservation{"path": obs.ByPath, "held": obs.Held, "mapped": obs.Mapped} {
		if got.Error != "" || got.ErrorCode != "" || got.NotExist || got.Data != value {
			return fmt.Errorf("%s observation mismatch: %+v", name, got)
		}
	}
	if obs.DirectoryError != "" || !slices.Equal(obs.Directory, []string{"entry"}) {
		return fmt.Errorf("directory mismatch: %+v", obs)
	}
	if !obs.Negative.NotExist || obs.Negative.Error == "" || obs.Negative.Data != "" {
		return fmt.Errorf("negative lookup mismatch: %+v", obs.Negative)
	}
	u := obs.Unlinked
	if allowDeletedOpenEIO && generation == 2 && u.ErrorCode == "EIO" && u.Error != "" && u.Data == "" && !u.NotExist {
		return nil
	}
	if u.Error != "" || u.ErrorCode != "" || u.NotExist || u.Data != value {
		return fmt.Errorf("deleted-open observation mismatch at generation %d: %+v", generation, u)
	}
	return nil
}
