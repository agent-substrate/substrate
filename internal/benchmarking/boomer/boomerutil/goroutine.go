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

package boomerutil

import (
	"runtime"
	"strconv"
	"strings"
)

// goroutineID extracts the runtime's per-goroutine ID via the standard
// runtime.Stack trick. Used to key per-VU state because boomer's Task model
// has no built-in per-VU hook — see the runtime.shutdown comment for the
// limitation this implies on user-count rescale.
func GoroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	line := string(buf[:n])
	const prefix = "goroutine "
	if !strings.HasPrefix(line, prefix) {
		return 0
	}
	end := strings.IndexByte(line[len(prefix):], ' ')
	if end < 0 {
		return 0
	}
	id, _ := strconv.ParseInt(line[len(prefix):len(prefix)+end], 10, 64)
	return id
}
