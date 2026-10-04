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

package storesql

import (
	"runtime"
	"time"
)

// Connection pool defaults for backends built on database/sql. They are the
// pgxpool defaults atepg runs with, so both backends hold and recycle
// connections alike.
const (
	ConnMaxLifetime = time.Hour
	ConnMaxIdleTime = 30 * time.Minute
)

// DefaultMaxConns is the pool size when none is configured: the pgxpool
// default of the larger of 4 and the CPU count.
func DefaultMaxConns() int {
	return max(4, runtime.NumCPU())
}
