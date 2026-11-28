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

// Package grpclb holds gRPC load balancing settings shared by Substrate's
// clients.
package grpclb

// RoundRobinServiceConfig is a gRPC service config that spreads RPCs over
// every address the resolver returns. gRPC's default, pick_first, sends all of
// a client's RPCs to the first address it connects to.
const RoundRobinServiceConfig = `{"loadBalancingConfig": [{"round_robin":{}}]}`
