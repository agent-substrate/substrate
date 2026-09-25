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

package main

import (
	"fmt"
	"os"
	"sort"

	"k8s.io/apimachinery/pkg/util/validation"

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
	// AllowedSecretNames, when set, narrows the grant to these Secret names in
	// every allowed namespace. SecretSelector narrows it to Secrets carrying all
	// of its labels. The criteria of one policy are AND-ed, so a policy with
	// both admits only a Secret that is named AND carries the labels. Write a
	// second policy to admit either.
	//
	// Leaving both unset grants every Secret in the allowed namespaces, which is
	// what a policy written before these fields existed already means. An empty
	// list or an empty selector is the same as leaving the field out: as with a
	// Kubernetes LabelSelector, an empty selector matches everything. There is
	// no way to write a grant that admits nothing; leave the policy out instead.
	AllowedSecretNames []string        `json:"allowedSecretNames,omitempty"`
	SecretSelector     *secretSelector `json:"secretSelector,omitempty"`
}

// secretSelector mirrors the shape of the Selector message the rest of Substrate
// uses (ateapi.Selector), so a policy selects Secrets the way an ActorTemplate
// selects workers. matchLabels is exact equality, as it is there. The shape is
// mirrored rather than imported: this is a config file, not the API.
type secretSelector struct {
	MatchLabels map[string]string `json:"matchLabels,omitempty"`
}

// labels returns the selector's labels, treating an absent selector as no
// narrowing rather than as a selector that matches nothing.
func (s *secretSelector) labels() map[string]string {
	if s == nil {
		return nil
	}
	return s.MatchLabels
}

// Decision is as much as a namespace and a Secret name can settle on their own.
type Decision int

const (
	// DecisionDeny: refuse without reading anything from Kubernetes.
	DecisionDeny Decision = iota
	// DecisionAllow: the grant admits this Secret by namespace or by name.
	DecisionAllow
	// DecisionCheckLabels: the grant is narrowed by label, so only the Secret's
	// own labels can settle it and it has to be read first.
	DecisionCheckLabels
)

// grant is what one atespace may read in one namespace.
type grant struct {
	// names is empty when the grant is not narrowed by name.
	names map[string]struct{}
	// labels is empty when the grant is not narrowed by label.
	labels map[string]string
}

// admitsName reports whether the grant's name criterion is satisfied. A grant
// that does not narrow by name admits every name.
func (g grant) admitsName(name string) bool {
	if len(g.names) == 0 {
		return true
	}
	_, ok := g.names[name]
	return ok
}

// NamespaceAuthorizer decides whether an atespace may resolve secrets in a given
// Kubernetes namespace. It is default-deny: an atespace absent from the mapping
// can resolve nothing.
type NamespaceAuthorizer struct {
	// allowed maps atespace -> namespace -> the grants that apply there. Each
	// policy contributes its own grant and they are OR-ed: a Secret is admitted
	// when any one of them admits it. Within a grant the criteria are AND-ed,
	// so every label in one selector must be present.
	allowed map[string]map[string][]grant
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
	allowed := make(map[string]map[string][]grant)
	for i, p := range file.Policies {
		if p.Atespace == "" {
			return nil, fmt.Errorf("namespace policy %d: atespace is required", i)
		}
		// A malformed name or label can never match, so it would narrow the
		// grant to nothing and look like the policy was simply ignored. Fail
		// loading instead.
		for _, n := range p.AllowedSecretNames {
			if len(validation.IsDNS1123Subdomain(n)) != 0 {
				return nil, fmt.Errorf("namespace policy %d: invalid secret name %q", i, n)
			}
		}
		for k, v := range p.SecretSelector.labels() {
			if len(validation.IsQualifiedName(k)) != 0 {
				return nil, fmt.Errorf("namespace policy %d: invalid label key %q", i, k)
			}
			if len(validation.IsValidLabelValue(v)) != 0 {
				return nil, fmt.Errorf("namespace policy %d: invalid label value %q for key %q", i, v, k)
			}
		}
		set := allowed[p.Atespace]
		if set == nil {
			set = make(map[string][]grant)
			allowed[p.Atespace] = set
		}
		// Each policy keeps its own grant. Merging them into one would AND the
		// selectors of separate policies together, and would let a narrow policy
		// take away the namespace a broad one granted.
		g := grant{names: map[string]struct{}{}, labels: map[string]string{}}
		for _, n := range p.AllowedSecretNames {
			g.names[n] = struct{}{}
		}
		for k, v := range p.SecretSelector.labels() {
			g.labels[k] = v
		}
		for _, ns := range p.AllowedNamespaces {
			set[ns] = append(set[ns], g)
		}
	}
	return &NamespaceAuthorizer{allowed: allowed}, nil
}

// Grants returns the loaded policy as atespace → sorted namespaces
func (a *NamespaceAuthorizer) Grants() map[string][]string {
	if a == nil {
		return nil
	}
	out := make(map[string][]string, len(a.allowed))
	for atespace, namespaces := range a.allowed {
		list := make([]string, 0, len(namespaces))
		for ns := range namespaces {
			list = append(list, ns)
		}
		sort.Strings(list)
		out[atespace] = list
	}
	return out
}

// Allowed reports whether atespace may resolve secrets in namespace. Default
// deny: an atespace absent from the mapping, or a namespace not in its list, is
// refused.
func (a *NamespaceAuthorizer) Allowed(atespace, namespace string) bool {
	set, ok := a.allowed[atespace]
	if !ok {
		return false
	}
	return len(set[namespace]) > 0
}

// Authorize settles as much as a namespace and a Secret name can, before
// anything is read from Kubernetes. DecisionCheckLabels means the grant is
// narrowed by label and only AllowedLabels can finish the decision.
func (a *NamespaceAuthorizer) Authorize(atespace, namespace, name string) Decision {
	labeled := false
	for _, g := range a.allowed[atespace][namespace] {
		if !g.admitsName(name) {
			continue
		}
		if len(g.labels) == 0 {
			// Every criterion this grant carries is satisfied.
			return DecisionAllow
		}
		labeled = true
	}
	if !labeled {
		return DecisionDeny
	}
	return DecisionCheckLabels
}

// AllowedLabels reports whether a Secret satisfies a grant that narrows by
// label. Every label in that grant must be present with the same value; the
// Secret may carry others. The name is needed as well as the labels: a grant
// that also narrows by name is satisfied only when both criteria hold, so a
// grant this name failed must not admit the Secret on its labels.
func (a *NamespaceAuthorizer) AllowedLabels(atespace, namespace, name string, labels map[string]string) bool {
	for _, g := range a.allowed[atespace][namespace] {
		if len(g.labels) == 0 || !g.admitsName(name) {
			continue
		}
		match := true
		for k, want := range g.labels {
			if got, ok := labels[k]; !ok || got != want {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
