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

package kubeprovider

import (
	"fmt"
	"os"

	"google.golang.org/protobuf/encoding/prototext"

	"github.com/agent-substrate/substrate/internal/proto/nsauthzpb"
)

// NamespaceAuthorizer decides whether an atespace may resolve secrets in a given
// Kubernetes namespace. It is default-deny: an atespace absent from the mapping
// can resolve nothing.
type NamespaceAuthorizer struct {
	// allowed maps atespace -> set of permitted namespaces.
	allowed map[string]map[string]struct{}
}

// LoadNamespaceAuthorizer reads a textproto NamespaceAuthorizationFile from path
// and builds an authorizer, so a malformed file fails startup rather than the
// first request.
func LoadNamespaceAuthorizer(path string) (*NamespaceAuthorizer, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading namespace policy file %q: %w", path, err)
	}
	var file nsauthzpb.NamespaceAuthorizationFile
	if err := prototext.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("parsing namespace policy file %q: %w", path, err)
	}
	return NewNamespaceAuthorizer(&file)
}

// NewNamespaceAuthorizer builds an authorizer over an already-parsed file,
// validating that each policy names an atespace.
func NewNamespaceAuthorizer(file *nsauthzpb.NamespaceAuthorizationFile) (*NamespaceAuthorizer, error) {
	allowed := make(map[string]map[string]struct{})
	for i, p := range file.GetPolicy() {
		if p.GetAtespace() == "" {
			return nil, fmt.Errorf("namespace policy %d: atespace is required", i)
		}
		set := allowed[p.GetAtespace()]
		if set == nil {
			set = make(map[string]struct{})
			allowed[p.GetAtespace()] = set
		}
		for _, ns := range p.GetAllowedNamespaces() {
			set[ns] = struct{}{}
		}
	}
	return &NamespaceAuthorizer{allowed: allowed}, nil
}

// Allowed reports whether atespace may resolve secrets in namespace. Default
// deny: an atespace absent from the mapping, or a namespace not in its list, is
// refused.
func (a *NamespaceAuthorizer) Allowed(atespace, namespace string) bool {
	set, ok := a.allowed[atespace]
	if !ok {
		return false
	}
	_, ok = set[namespace]
	return ok
}
