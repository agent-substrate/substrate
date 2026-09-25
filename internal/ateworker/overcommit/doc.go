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

// Package overcommit runs worker-level overcommit inside an ateworker. The
// control plane books an actor's full limits against its Worker when it binds
// the actor; the Controller here watches what the worker's actors actually
// use and moves the Worker's allocation toward the predicted usage, handing
// back what they are not expected to need and claiming more when they are.
//
// The Controller learns which actors the worker hosts, and what each one
// declared, from the same stats samples it measures usage with.
package overcommit
