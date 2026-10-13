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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/hardware"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// cpuFeaturesTimeout bounds one `runsc cpu-features` run, which only reads
// CPUID. A var so tests can shorten it.
var cpuFeaturesTimeout = 30 * time.Second

// runscCPUFeatures probes a gVisor sandbox version by running its runsc's
// cpu-features command: the CPU features that runsc exposes to guests on this
// host. gVisor restores a checkpoint only on a host offering every feature
// the checkpoint was taken with, so a snapshot carries this set and restores
// only where it matches.
type runscCPUFeatures struct{}

func (runscCPUFeatures) Probe(ctx context.Context, v *ateletpb.SandboxVersion) ([]*ateletpb.AttributeEntry, error) {
	runsc := runscPathFor(v.GetAssetPaths())
	if runsc == "" {
		return nil, fmt.Errorf("sandbox version %q has no runsc asset", v.GetName())
	}
	ctx, cancel := context.WithTimeout(ctx, cpuFeaturesTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, runsc, "cpu-features").Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && len(exitErr.Stderr) > 0 {
			return nil, fmt.Errorf("while running %s cpu-features: %w: %s", runsc, err, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return nil, fmt.Errorf("while running %s cpu-features: %w", runsc, err)
	}
	hash, err := cpuFeaturesHash(string(out))
	if err != nil {
		return nil, fmt.Errorf("%s cpu-features: %w", runsc, err)
	}
	return []*ateletpb.AttributeEntry{{Key: hardware.AttrCPUFeatures, Value: hash}}, nil
}

// runscPathFor returns runsc's local path among a version's assets: the
// "gvisor" release tarball's runsc, or a bare "runsc" binary from configs
// that predate the tarball. atelet hands both out as runsc's own path.
func runscPathFor(paths map[string]string) string {
	if p := paths["gvisor"]; p != "" {
		return p
	}
	return paths["runsc"]
}

// cpuFeaturesHash is the hex SHA-256 of the comma-separated feature list
// `runsc cpu-features` prints, sorted and deduplicated: the set, whatever
// order runsc prints it in. The list itself runs past the 256 characters an
// attribute value may hold.
func cpuFeaturesHash(out string) (string, error) {
	var features []string
	for _, f := range strings.Split(out, ",") {
		if f = strings.TrimSpace(f); f != "" {
			features = append(features, f)
		}
	}
	if len(features) == 0 {
		return "", errors.New("reported no CPU features")
	}
	slices.Sort(features)
	features = slices.Compact(features)
	sum := sha256.Sum256([]byte(strings.Join(features, ",")))
	return hex.EncodeToString(sum[:]), nil
}
