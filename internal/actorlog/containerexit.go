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

package actorlog

import (
	"time"

	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/resources"
)

// ContainerExitedMessage is the message of the record EmitContainerExit writes.
const ContainerExitedMessage = "Actor container exited"

// ContainerExit is the exit of one container of an actor, as its worker saw it.
type ContainerExit struct {
	Container string
	// ExitCode is nil when the runtime gave none. A runtime can report a signal
	// as a code above 128, so exit(139) and a SIGSEGV can read the same.
	ExitCode *int32
}

// EmitContainerExit logs that a container of the actor exited. The record is a
// WARN for each exit code: the template does not say if a container is expected
// to finish, so the reader uses the code to tell. The level field has the name
// and value that the worker's slog records use.
//
// Like a lifecycle event, the record has no container label, because the worker
// wrote it. The container name is a field of the record.
func (al *ActorLogger) EmitContainerExit(a resources.ActorAttribution, e ContainerExit) {
	envelope := map[string]any{
		"time":       time.Now().Format(time.RFC3339Nano),
		"level":      "WARN",
		"message":    ContainerExitedMessage,
		al.labelsKey: ateattr.ActorLogLabels(a, ""),
		string(ateattr.ActorExitedContainerNameKey): e.Container,
	}
	if e.ExitCode != nil {
		envelope[string(ateattr.ActorExitedContainerExitCodeKey)] = *e.ExitCode
	}
	al.write(envelope)
}
