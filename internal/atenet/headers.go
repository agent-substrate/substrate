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

// Package atenet defines the shared contract for Substrate actor networking.
package atenet

const (
	// ActorNameHeader and AtespaceHeader identify the actor selected for ingress
	// routing. HTTP field names are case-insensitive; these use their HTTP/2 wire
	// form so dataplane configuration and header mutations are native.
	ActorNameHeader = "x-ate-actor-name"
	AtespaceHeader  = "x-ate-atespace"
)
