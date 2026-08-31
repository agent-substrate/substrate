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

package resources

// ActorAttribution is what telemetry about an actor is attributed to: an
// ActorRef plus the two things a ref does not carry, the server-assigned uid and
// the template the actor was built from. Shared by ateattr, actorlog, and
// ateom's usage sampling so those producers cannot drift apart.
//
// Unrelated to the credential sense of "actor identity" elsewhere in the repo
// (ateapi's ActorIdentity service, substratex509, ateompath.ActorIdentityDirPath)
// — nothing here is a secret or is presented as proof of anything.
type ActorAttribution struct {
	Ref               ActorRef
	UID               string
	TemplateNamespace string
	TemplateName      string
}
