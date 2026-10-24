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
	"log/slog"
	"os"
	"sort"
	"sync"

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
	// SecretSelector, when set, narrows the grant to Secrets in every allowed
	// namespace that carry all of its labels.
	//
	// Leaving it unset grants every Secret in the allowed namespaces, which is
	// what a policy written before this field existed already means. An empty
	// selector is the same as leaving the field out: as with a Kubernetes
	// LabelSelector, an empty selector matches everything. There is no way to
	// write a grant that admits nothing; leave the policy out instead.
	SecretSelector *secretSelector `json:"secretSelector,omitempty"`
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

// grant is what one atespace may read in one namespace.
type grant struct {
	// labels is empty when the grant is not narrowed by label.
	labels map[string]string
}

// NamespaceAuthorizer decides whether an atespace may resolve secrets in a given
// Kubernetes namespace. It is default-deny: an atespace absent from the mapping
// can resolve nothing. One loaded from a file rereads the file when it changes.
type NamespaceAuthorizer struct {
	// path is the policy file; empty when the authorizer was built from an
	// already parsed policy and never reloads.
	path string

	mu sync.Mutex
	// fi is the stat of path taken before its last parse, successful or not,
	// so a file that fails to parse is read and logged once rather than per
	// request. Nil when path could not be stat'ed, so the next call retries.
	fi os.FileInfo
	// loadErr is why the file as it is now failed to load; allowed then holds
	// the last policy that loaded.
	loadErr error
	// allowed maps atespace -> namespace -> the grants that apply there. Each
	// policy contributes its own grant and they are OR-ed: a Secret is admitted
	// when any one of them admits it. Within a grant the criteria are AND-ed,
	// so every label in one selector must be present.
	allowed map[string]map[string][]grant
}

// LoadNamespaceAuthorizer reads the YAML policy file at path and builds an
// authorizer that rereads the file whenever it changes. A malformed file fails
// here, at startup; once running, a file that fails to load leaves the last
// good policy in force.
func LoadNamespaceAuthorizer(path string) (*NamespaceAuthorizer, error) {
	a := &NamespaceAuthorizer{path: path}
	if _, err := a.policy(); err != nil {
		return nil, err
	}
	return a, nil
}

// readNamespacePolicy parses the YAML policy file at path.
func readNamespacePolicy(path string) (*NamespaceAuthorizer, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading namespace policy file %q: %w", path, err)
	}
	var file namespacePolicyFile
	// Strict: a typo like matchLabel, or an unsupported field like
	// matchExpressions, must fail rather than silently parse as an empty
	// selector, which would grant every Secret in the namespace.
	if err := yaml.UnmarshalStrict(data, &file); err != nil {
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
		// A malformed label can never match, so it would narrow the grant to
		// nothing and look like the policy was simply ignored. Fail loading
		// instead.
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
		g := grant{labels: map[string]string{}}
		for k, v := range p.SecretSelector.labels() {
			g.labels[k] = v
		}
		for _, ns := range p.AllowedNamespaces {
			set[ns] = append(set[ns], g)
		}
	}
	return &NamespaceAuthorizer{allowed: allowed}, nil
}

// policy returns the grants in force, first rereading the policy file if its
// stat no longer matches the last one parsed. The kubelet updates a ConfigMap
// volume by swapping in a new file, so the stat changes even when the size
// does not. The error is why the file as it is now failed to load.
func (a *NamespaceAuthorizer) policy() (map[string]map[string][]grant, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.path == "" {
		return a.allowed, nil
	}
	fi, err := os.Stat(a.path)
	if err != nil {
		a.fi = nil
		return a.allowed, a.loadFailed(fmt.Errorf("reading namespace policy file %q: %w", a.path, err))
	}
	if a.fi != nil && os.SameFile(a.fi, fi) && fi.ModTime().Equal(a.fi.ModTime()) && fi.Size() == a.fi.Size() {
		return a.allowed, a.loadErr
	}
	a.fi = fi
	next, err := readNamespacePolicy(a.path)
	if err != nil {
		return a.allowed, a.loadFailed(err)
	}
	a.allowed, a.loadErr = next.allowed, nil
	slog.Info("loaded namespace authorization policy", slog.String("file", a.path))
	return a.allowed, nil
}

// loadFailed records err as the load error and returns it. It logs err unless
// it repeats the last one or no policy has loaded yet, when
// LoadNamespaceAuthorizer returns it instead. The caller holds a.mu.
func (a *NamespaceAuthorizer) loadFailed(err error) error {
	if a.allowed != nil && (a.loadErr == nil || a.loadErr.Error() != err.Error()) {
		slog.Error("namespace policy file failed to load, keeping the last policy that loaded", slog.String("file", a.path), slog.Any("err", err))
	}
	a.loadErr = err
	return err
}

// LoadError reports why the policy file failed to load, or nil when the
// policy in force is the file's current content.
func (a *NamespaceAuthorizer) LoadError() error {
	if a == nil {
		return nil
	}
	_, err := a.policy()
	return err
}

// Grants returns the loaded policy as atespace → sorted namespaces
func (a *NamespaceAuthorizer) Grants() map[string][]string {
	if a == nil {
		return nil
	}
	allowed, _ := a.policy()
	out := make(map[string][]string, len(allowed))
	for atespace, namespaces := range allowed {
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
	allowed, _ := a.policy()
	set, ok := allowed[atespace]
	if !ok {
		return false
	}
	return len(set[namespace]) > 0
}

// AllowedSecret reports whether atespace may resolve a Secret with these
// labels in namespace. A grant without a label selector admits every Secret,
// including one that does not exist: pass nil labels to ask that question
// before reading anything from Kubernetes. The namespace grant is checked again
// here, so a request that straddles a reload is settled by the newer policy.
func (a *NamespaceAuthorizer) AllowedSecret(atespace, namespace string, labels map[string]string) bool {
	allowed, _ := a.policy()
	for _, g := range allowed[atespace][namespace] {
		if len(g.labels) == 0 {
			return true
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
