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

	"sigs.k8s.io/yaml"
)

// namespacePolicyFile is the YAML the authorizer loads: a list of grants, each
// mapping one atespace to the namespaces whose Secrets it may resolve.
type namespacePolicyFile struct {
	Policies []atespaceNamespacePolicy `json:"policies"`
}

type atespaceNamespacePolicy struct {
	Atespace          string   `json:"atespace"`
	AllowedNamespaces []string `json:"allowedNamespaces"`
}

// NamespaceAuthorizer decides whether an atespace may resolve secrets in a given
// Kubernetes namespace. It is default-deny: an atespace absent from the mapping
// can resolve nothing.
type NamespaceAuthorizer struct {
	// allowed maps atespace -> set of permitted namespaces.
	allowed map[string]map[string]struct{}
}

// LoadNamespaceAuthorizer reads the YAML policy file at path and builds an
// authorizer, so a malformed file fails startup rather than the first request.
func LoadNamespaceAuthorizer(path string) (*NamespaceAuthorizer, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading namespace policy file %q: %w", path, err)
	}
	var file namespacePolicyFile
	if err := yaml.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("parsing namespace policy file %q: %w", path, err)
	}
	return newNamespaceAuthorizer(file)
}

// newNamespaceAuthorizer builds an authorizer over a parsed policy file,
// validating that each grant names an atespace.
func newNamespaceAuthorizer(file namespacePolicyFile) (*NamespaceAuthorizer, error) {
	allowed := make(map[string]map[string]struct{})
	for i, p := range file.Policies {
		if p.Atespace == "" {
			return nil, fmt.Errorf("namespace policy %d: atespace is required", i)
		}
		set := allowed[p.Atespace]
		if set == nil {
			set = make(map[string]struct{})
			allowed[p.Atespace] = set
		}
		for _, ns := range p.AllowedNamespaces {
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
