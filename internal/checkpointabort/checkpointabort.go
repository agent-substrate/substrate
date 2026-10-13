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

// Package checkpointabort defines how a checkpoint reports that it gave up
// without changing the actor: the workload still runs, its tunnel still serves
// it, and nothing was written. The worker's CheckpointWorkload returns Code for
// that and for nothing else, atelet's Checkpoint passes it through unchanged,
// and ate-api-server then returns the actor to the state it had before the
// operation instead of crashing it.
//
// Only an MSHV MEMORY checkpoint returns it today, when freezing the guest's
// containers fails, every container it froze was thawed again, and no earlier
// attempt of the checkpoint deactivated the actor's tunnel.
package checkpointabort

import (
	"go.ate.dev/substrate/internal/apierror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Code is the gRPC code of an aborted checkpoint. The Checkpoint RPCs return
// it for nothing else, and the gRPC framework never generates it.
const Code = codes.Aborted

// Errorf returns an aborted-checkpoint error, which a server reports with Code.
func Errorf(format string, args ...any) error {
	return apierror.Aborted(format, args...)
}

// Is reports whether err, returned by Errorf or received from a Checkpoint
// RPC, reports an aborted checkpoint.
func Is(err error) bool {
	return err != nil && (status.Code(err) == Code || apierror.Code(err) == Code)
}
