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

// Package lint runs a set of predicates ("rules") against a model.API,
// catching API-shape mistakes.
package lint

import (
	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

// Finding is one rule violation: which API entity it's about, and why.
type Finding struct {
	Subject string
	Message string
}

// Rule is one predicate over the API model. Check returns one Finding per
// violation (nil if api satisfies the rule), or a non-nil error if the rule
// could not be evaluated at all - a failure of the rule's own machinery,
// not a finding about api.
type Rule struct {
	Name        string
	Description string
	Check       func(api *model.API) ([]Finding, error)
}
