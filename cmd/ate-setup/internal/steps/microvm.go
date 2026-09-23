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

package steps

import (
	"context"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// installMicrovmDepScript assembles the micro-VM guest assets, stages them in
// the object store the atelet pulls from, and applies the cluster-wide
// `microvm` SandboxConfig. It is invoked rather than reimplemented for the
// reason given above deployLocustScript: the asset assembly and object-store
// staging it drives are out of scope for this command.
const installMicrovmDepScript = "hack/install-microvm-deps.sh"

// DeployMicroVMDeps installs the micro-VM prerequisites: the guest assets and
// the cluster-wide `microvm` SandboxConfig.
//
// `deploy ate-system` installs only the gvisor-default SandboxConfig, so
// anything that asks for micro-VM workers needs this first: the micro-VM
// demos, a micro-VM benchmark run, or a WorkerPool of your own. It is a step
// of its own rather than something each of those does for itself because
// making a cluster micro-VM capable is worth doing without also deploying a
// workload onto it.
//
// Re-running is cheap: the script leaves assets alone when their recorded
// versions still match, and re-applies the SandboxConfig either way.
func (e *Env) DeployMicroVMDeps(ctx context.Context) error {
	log.Step("deploy_microvm_deps")
	return e.runScript(ctx, installMicrovmDepScript, "--install")
}

// DeleteMicroVMDeps removes the cluster-wide `microvm` SandboxConfig.
//
// The staged assets are deliberately left in the object store. The bucket
// outlives any one install, and re-staging is the slow half of
// DeployMicroVMDeps.
func (e *Env) DeleteMicroVMDeps(ctx context.Context) error {
	log.Step("delete_microvm_deps")
	return e.runScript(ctx, installMicrovmDepScript, "--delete")
}
