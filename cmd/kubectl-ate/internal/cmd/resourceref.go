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

package cmd

import (
	"fmt"
	"strings"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// parseAtespacedName parses a reference to an atespace-scoped resource,
// either as <atespace>/<name> or as <name>, which defaults to defaultAtespace
func parseAtespacedName(value, defaultAtespace string) (*ateapipb.ObjectRef, error) {
	atespace, name, ok := strings.Cut(value, "/")
	if !ok {
		atespace, name = defaultAtespace, value
	}
	if !resources.IsValidResourceName(atespace) || !resources.IsValidResourceName(name) {
		return nil, fmt.Errorf("malformed reference %q (expected <name> or <atespace>/<name>)", value)
	}
	return &ateapipb.ObjectRef{Atespace: atespace, Name: name}, nil
}
