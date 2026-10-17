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

package config

import "fmt"

// RequiredError reports a setting no channel supplied.
//
// The message names all three channels because the reader may know only one of
// them: telling someone who configures by file to set an environment variable
// sends them to the wrong place.
type RequiredError struct {
	Setting Setting
}

func (e *RequiredError) Error() string {
	return fmt.Sprintf("at least one of %s must be set, got none", e.Setting.Channels())
}

// InvalidError reports a value that is not valid for its setting, naming the
// channel it came from so the reader knows which one to edit.
type InvalidError struct {
	Value Value
	// Want describes the accepted values, e.g. "envoy or agentgateway".
	Want string
}

func (e *InvalidError) Error() string {
	return fmt.Sprintf("%s must be %s, got %q (from %s)",
		e.Value.Setting.Key, e.Want, e.Value.Display(), e.Value.From.Describe(e.Value.Setting))
}

// ConflictError reports two values that cannot hold together, naming the
// channel each came from.
type ConflictError struct {
	A, B Value
	// Why states the rule that was broken.
	Why string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("%s=%q (from %s) conflicts with %s=%q (from %s): %s",
		e.A.Setting.Key, e.A.Display(), e.A.From.Describe(e.A.Setting),
		e.B.Setting.Key, e.B.Display(), e.B.From.Describe(e.B.Setting),
		e.Why)
}
