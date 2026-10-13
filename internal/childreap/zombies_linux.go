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

//go:build linux

package childreap

import (
	"bytes"
	"os"
	"strconv"
	"strings"
)

// zombieChildren lists the zombie children of parent, from /proc.
func zombieChildren(parent int) ([]int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	var zombies []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		stat, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			// Gone between the listing and the read.
			continue
		}
		if state, ppid, ok := parseStat(stat); ok && state == 'Z' && ppid == parent {
			zombies = append(zombies, pid)
		}
	}
	return zombies, nil
}

// parseStat returns the state and parent pid of a /proc/<pid>/stat line. The
// command name before them may hold spaces and parentheses, so the fields are
// read after its closing parenthesis.
func parseStat(stat []byte) (state byte, ppid int, ok bool) {
	i := bytes.LastIndexByte(stat, ')')
	if i < 0 {
		return 0, 0, false
	}
	fields := strings.Fields(string(stat[i+1:]))
	if len(fields) < 2 || len(fields[0]) != 1 {
		return 0, 0, false
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, 0, false
	}
	return fields[0][0], ppid, true
}
