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

package actornet

import (
	"fmt"
	"os"
)

// WriteRootfsEtcFiles writes the sandbox's /etc/resolv.conf and /etc/hosts
// into rootfs, pointing the resolver at the actor's gateway.
//
// rootfs must be the base layer under the actor's restored rootfs writes, so
// rewriting it on every activation keeps the actor's own edits. Writing these
// after restore instead would clobber them.
func WriteRootfsEtcFiles(rootfs, hostname string) error {
	pod, err := os.ReadFile("/etc/resolv.conf")
	if err != nil {
		return fmt.Errorf("reading the worker pod resolv.conf: %w", err)
	}
	if err := WriteRootfsResolvConf(rootfs, SandboxResolvConf(ActorVethGateway, pod)); err != nil {
		return err
	}
	return WriteRootfsHosts(rootfs, SandboxHosts(hostname, ActorVethIP))
}
