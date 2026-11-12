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

package shutdown

import "time"

// SignalDeliveryTimeout bounds delivering one signal to a workload during
// graceful shutdown: a SignalProcess ttrpc round-trip on microVM, or a `runsc
// kill` invocation on gVisor. It is separate from the grace period so a wedged
// transport cannot spend the actor's save budget or stall longer when the grace
// period grows.
const SignalDeliveryTimeout = 10 * time.Second
