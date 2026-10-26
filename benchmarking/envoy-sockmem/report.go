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
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

// labeled is a client result with the run's label, as run-docker.sh writes
// it to results.jsonl.
type labeled struct {
	Label string `json:"label"`
	result
}

// writeReport prints the results in r as a table: memory growth for the slow
// modes, throughput and CPU for the others.
func writeReport(w io.Writer, r io.Reader) error {
	lines := bufio.NewScanner(r)
	lines.Buffer(make([]byte, 64<<10), 1<<20)
	mib := func(a, b uint64) float64 { return (float64(a) - float64(b)) / (1 << 20) }
	fmt.Fprintf(w, "%-27s %-7s %-15s %6s %6s %5s | %9s %9s %9s | %9s %9s | %8s %8s\n",
		"settings", "kind", "mode", "conns", "opened", "errs", "cg +MiB", "sock +MiB", "heap +MiB", "cg KiB/c", "sock KiB/c", "MB/s", "cpu s/GB")
	for lines.Scan() {
		var l labeled
		if err := json.Unmarshal(lines.Bytes(), &l); err != nil {
			return err
		}
		b, h := l.Before, l.Holding
		line := fmt.Sprintf("%-27s %-7s %-15s %6d %6d %5d | ", l.Label, l.Kind, l.Mode, l.Conns, l.Opened, l.Errors)
		if l.MBPerSec > 0 {
			line += fmt.Sprintf("%9s %9s %9s | %9s %9s | %8.0f %8.2f", "", "", "", "", "", l.MBPerSec, l.EnvoyCPUSecPerGB)
		} else {
			cg, sock, heap := mib(h.CgroupBytes, b.CgroupBytes), mib(h.CgroupSockBytes, b.CgroupSockBytes), mib(h.HeapAllocated, b.HeapAllocated)
			perConn := func(v float64) float64 {
				if l.Opened == 0 {
					return 0
				}
				return v * 1024 / float64(l.Opened)
			}
			line += fmt.Sprintf("%9.1f %9.1f %9.1f | %9.1f %9.1f |", cg, sock, heap, perConn(cg), perConn(sock))
		}
		if l.FirstError != "" {
			line += "  first error: " + l.FirstError
		}
		fmt.Fprintln(w, line)
	}
	return lines.Err()
}
