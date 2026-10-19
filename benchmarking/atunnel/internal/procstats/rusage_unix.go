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

//go:build linux || darwin

package procstats

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

func cpuSeconds() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return tv(ru.Utime) + tv(ru.Stime)
}

func tv(t syscall.Timeval) float64 {
	return float64(t.Sec) + float64(t.Usec)/1e6
}

// rss returns current RSS from /proc on Linux, and peak RSS from getrusage
// elsewhere, where current RSS needs a Mach call.
func rss() (uint64, bool) {
	if runtime.GOOS == "linux" {
		if b, err := os.ReadFile("/proc/self/status"); err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				if v, ok := strings.CutPrefix(line, "VmRSS:"); ok {
					kb, _ := strconv.ParseUint(strings.TrimSuffix(strings.TrimSpace(v), " kB"), 10, 64)
					return kb * 1024, false
				}
			}
		}
	}
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, true
	}
	maxrss := uint64(ru.Maxrss)
	if runtime.GOOS == "linux" {
		// Linux reports kilobytes; macOS reports bytes.
		maxrss *= 1024
	}
	return maxrss, true
}
