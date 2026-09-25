//go:build linux

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
	"fmt"
	"os"

	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/atunnel"
)

// writeActorResolvConf points the guest resolver at its fixed gateway address.
func writeActorResolvConf(rootfs string) error {
	pod, err := os.ReadFile("/etc/resolv.conf")
	if err != nil {
		return fmt.Errorf("reading the worker pod resolv.conf: %w", err)
	}
	return ateomnet.WriteRootfsResolvConf(rootfs, ateomnet.SandboxResolvConf(pod))
}

// attachAtunnel completes setup after atunnel receives the service's dialer.
func (s *AteomService) attachAtunnel(ingress *atunnel.Server, egress *atunnel.Egress, egressPort uint16) {
	s.atunnelIngress = ingress
	s.atunnelEgress = egress
	s.atunnelEgressPort = egressPort
}
