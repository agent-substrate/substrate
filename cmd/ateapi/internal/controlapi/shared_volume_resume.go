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

package controlapi

import (
	"context"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"time"
)

// releaseFailedResumeClaims retains ownership unless both possible access
// paths (the workload and the node's attachment) have been shut down.
func (w *ActorWorkflow) releaseFailedResumeClaims(ctx context.Context, ref resources.ActorRef, actor *ateapipb.Actor, template *ateapipb.ActorTemplate) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if err := w.ensureAteletTerminated(ctx, ref, actor, template, ateattr.OperationResume); err != nil {
		return err
	}
	if err := detachActorVolumes(ctx, w.pluginRegistry, actor, template, ateattr.OperationResume); err != nil {
		return err
	}
	return deactivateSharedVolumes(ctx, w.store, actor)
}
