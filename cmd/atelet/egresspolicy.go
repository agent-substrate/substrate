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
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// toAteletEgressPolicy copies the destinations each rule of the control
// plane's policy allows. Metadata and rule effects are dropped: the worker
// does not apply them, and they name credentials it has no need to see.
func toAteletEgressPolicy(in *ateapipb.EgressPolicy) *ateletpb.EgressPolicy {
	if in == nil {
		return nil
	}
	out := &ateletpb.EgressPolicy{Rules: make([]*ateletpb.EgressRule, 0, len(in.GetRules()))}
	for _, r := range in.GetRules() {
		rule := &ateletpb.EgressRule{}
		if h := r.GetHttp(); h != nil {
			rule.Http = toEgressDestinations(h.GetHostnames(), h.GetPorts())
		}
		if h := r.GetHttps(); h != nil {
			rule.Https = toEgressDestinations(h.GetHostnames(), h.GetPorts())
		}
		if t := r.GetTlsPassthrough(); t != nil {
			rule.TlsPassthrough = toEgressDestinations(t.GetHostnames(), t.GetPorts())
		}
		out.Rules = append(out.Rules, rule)
	}
	return out
}

func toEgressDestinations(hostnames []string, ports *ateapipb.Ports) *ateletpb.EgressDestinations {
	out := &ateletpb.EgressDestinations{Hostnames: hostnames}
	if ports != nil {
		out.Ports = &ateletpb.EgressPorts{Numbers: ports.GetNumbers()}
		if ports.GetAll() != nil {
			out.Ports.All = &ateletpb.EgressAllPorts{}
		}
	}
	return out
}
