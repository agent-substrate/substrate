//go:build linux

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

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

const (
	maxMapCountPath = "/proc/sys/vm/max_map_count"

	// recommendedMaxMapCount is the vm.max_map_count gVisor recommends. Below
	// it, large builds in a sandbox can crash with a fault, such as the Go linker.
	recommendedMaxMapCount = 4194304
)

// warnLowMaxMapCount logs an error when the host's vm.max_map_count is below
// gVisor's recommendation. runsc runs with --host-settings=ignore, so it does
// not report this itself.
func warnLowMaxMapCount(ctx context.Context, path string) {
	got, err := readMaxMapCount(path)
	if err != nil {
		slog.WarnContext(ctx, "Cannot check vm.max_map_count", slog.Any("err", err))
		return
	}
	if got < recommendedMaxMapCount {
		slog.ErrorContext(ctx, "vm.max_map_count is below the gVisor recommendation, large builds in actors can crash; raise it on the node",
			slog.Int64("got", got), slog.Int64("want", recommendedMaxMapCount))
	}
}

func readMaxMapCount(path string) (int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing %s: %w", path, err)
	}
	return n, nil
}
