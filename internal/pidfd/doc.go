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

// Package pidfd watches for processes to exit without holding a thread per
// process, so a worker can watch thousands of them.
//
// It needs Linux 5.3+ for pidfd_open. Before 5.10, which added
// PIDFD_NONBLOCK, each watch falls back to a blocking poll on its own thread.
package pidfd
