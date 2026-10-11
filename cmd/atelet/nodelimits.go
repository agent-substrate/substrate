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
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// inotifyInstancesFloor is the fs.inotify.max_user_instances below which
// atelet warns: kubelet, containerd and every sandbox share that budget, and a
// node at the default of 128 ran out near 64 sandboxes.
const inotifyInstancesFloor = 8192

// logNodeLimits logs the page size, which runsc builds and snapshots are tied
// to, and the inotify limits. atelet is unprivileged and cannot raise them.
func logNodeLimits(ctx context.Context, procSys string) {
	instances := readProcInt(procSys + "/fs/inotify/max_user_instances")
	watches := readProcInt(procSys + "/fs/inotify/max_user_watches")
	attrs := []any{
		slog.Int("page_size", os.Getpagesize()),
		slog.Int("inotify_max_user_instances", instances),
		slog.Int("inotify_max_user_watches", watches),
	}
	if instances > 0 && instances < inotifyInstancesFloor {
		slog.WarnContext(ctx, "fs.inotify.max_user_instances is low; sandboxes fail to start when the node runs out: set it to 8192 or more", attrs...)
		return
	}
	slog.InfoContext(ctx, "node limits", attrs...)
}

// readProcInt returns the integer in a /proc/sys file, or 0 if unreadable.
func readProcInt(path string) int {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0
	}
	return n
}
