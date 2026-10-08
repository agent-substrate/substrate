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

// Package fakeworker holds what the two programs of the benchmark fake data
// plane share: how a fake Worker is named and placed.
package fakeworker

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// MaxRunLength bounds the run prefix so every generated name stays well inside
// the 63-character limit on Worker names.
const MaxRunLength = 8

// WorkerPoolLabel is the label key the WorkerPool controller selects a pool's
// worker pods by; a fake pool's status.selector names it the same way.
const WorkerPoolLabel = "ate.dev/worker-pool"

var runPattern = regexp.MustCompile(`^[a-z0-9]+$`)

// podUIDNamespace is the UUIDv5 namespace fake WorkerPodUids are derived in.
// Changing it renames every fake Worker's pod UID.
var podUIDNamespace = uuid.MustParse("6f1c2a5e-8b3d-4e7a-9c01-5d2b7e4f8a90")

// ValidateRun reports whether run is usable as a run prefix.
func ValidateRun(run string) error {
	if len(run) == 0 || len(run) > MaxRunLength || !runPattern.MatchString(run) {
		return fmt.Errorf("run prefix %q must be 1 to %d lowercase letters or digits", run, MaxRunLength)
	}
	return nil
}

// Prefix is the name prefix every fake Worker of run shares.
func Prefix(run string) string {
	return "fake-" + run + "-"
}

// Name returns the name of the index-th fake Worker of a WorkerPool. The pool
// enters as a hash because its namespace and name together can exceed the
// 63-character limit on Worker names.
func Name(run, namespace, pool string, index int) string {
	return poolPrefix(run, namespace, pool) + strconv.Itoa(index)
}

// Index returns the index of the fake Worker named name in a WorkerPool, and
// false when the name is not one of that pool's fake Workers.
func Index(run, namespace, pool, name string) (int, bool) {
	rest, ok := strings.CutPrefix(name, poolPrefix(run, namespace, pool))
	if !ok {
		return 0, false
	}
	i, err := strconv.Atoi(rest)
	if err != nil || i < 0 || strconv.Itoa(i) != rest {
		return 0, false
	}
	return i, true
}

func poolPrefix(run, namespace, pool string) string {
	sum := sha256.Sum256([]byte(namespace + "/" + pool))
	return Prefix(run) + hex.EncodeToString(sum[:])[:8] + "-"
}

// Node returns the node the index-th fake Worker of a pool is placed on:
// round robin over nodes, which the caller keeps sorted.
func Node(index int, nodes []string) string {
	return nodes[index%len(nodes)]
}

// PodUID returns the WorkerPodUid recorded for the Worker named name.
func PodUID(name string) string {
	return uuid.NewSHA1(podUIDNamespace, []byte(name)).String()
}

// IP returns the address recorded for the index-th fake Worker of a pool: a
// documentation-range address that nothing answers on. Addresses repeat past
// 254 Workers, which ate-api-server allows.
func IP(index int) string {
	return "192.0.2." + strconv.Itoa(index%254+1)
}
