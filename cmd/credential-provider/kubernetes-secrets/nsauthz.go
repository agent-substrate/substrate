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
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

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
	// OwnNamespaceSelector, when set, takes the place of atespace and
	// allowedNamespaces: every namespace whose labels it matches may be read
	// by the atespace of the same name, and by no other. The namespaces are
	// matched against their labels at the time of each request, so one that is
	// created, labeled or unlabeled after startup is granted or refused
	// without a restart. It must name at least one requirement.
	OwnNamespaceSelector *metav1.LabelSelector `json:"ownNamespaceSelector,omitempty"`
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

// selectorGrant is the grant a namespace selector gives in each namespace it
// matches.
type selectorGrant struct {
	selector labels.Selector
	grant    grant
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
	// ownNamespace holds the ownNamespaceSelector policies, OR-ed with allowed.
	ownNamespace []selectorGrant
	// namespaces is the cache the selectors are matched against.
	// WatchNamespaces sets it; until then those policies grant nothing.
	namespaces corelisters.NamespaceLister
}

// LoadNamespaceAuthorizer reads the YAML policy file at path and builds an
// authorizer, so a malformed file fails startup rather than the first request.
func LoadNamespaceAuthorizer(path string) (*NamespaceAuthorizer, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading namespace policy file %q: %w", path, err)
	}
	var file namespacePolicyFile
	// Strict: a typo like matchLabel, or a field secretSelector does not
	// support like matchExpressions, must fail rather than silently parse as
	// an empty selector, which would grant every Secret in the namespace.
	if err := yaml.UnmarshalStrict(data, &file); err != nil {
		return nil, fmt.Errorf("parsing namespace policy file %q: %w", path, err)
	}
	return newNamespaceAuthorizer(file)
}

// newNamespaceAuthorizer builds an authorizer over a parsed policy file,
// validating that each grant names an atespace or selects namespaces.
func newNamespaceAuthorizer(file namespacePolicyFile) (*NamespaceAuthorizer, error) {
	allowed := make(map[string]map[string][]grant)
	var ownNamespace []selectorGrant
	for i, p := range file.Policies {
		if p.OwnNamespaceSelector != nil {
			if p.Atespace != "" || len(p.AllowedNamespaces) != 0 {
				return nil, fmt.Errorf("namespace policy %d: ownNamespaceSelector cannot be combined with atespace or allowedNamespaces", i)
			}
			g, err := newGrant(i, p.SecretSelector)
			if err != nil {
				return nil, err
			}
			sg, err := newSelectorGrant(i, p.OwnNamespaceSelector, g)
			if err != nil {
				return nil, err
			}
			ownNamespace = append(ownNamespace, sg)
			continue
		}
		if p.Atespace == "" {
			return nil, fmt.Errorf("namespace policy %d: atespace is required", i)
		}
		g, err := newGrant(i, p.SecretSelector)
		if err != nil {
			return nil, err
		}
		set := allowed[p.Atespace]
		if set == nil {
			set = make(map[string][]grant)
			allowed[p.Atespace] = set
		}
		for _, ns := range p.AllowedNamespaces {
			set[ns] = append(set[ns], g)
		}
	}
	return &NamespaceAuthorizer{allowed: allowed, ownNamespace: ownNamespace}, nil
}

// newSelectorGrant gives grant g of policy i in the namespaces its selector
// matches.
func newSelectorGrant(i int, ls *metav1.LabelSelector, g grant) (selectorGrant, error) {
	selector, err := metav1.LabelSelectorAsSelector(ls)
	if err != nil {
		return selectorGrant{}, fmt.Errorf("namespace policy %d: invalid ownNamespaceSelector: %w", i, err)
	}
	// An empty selector matches every namespace. Ask for that by label.
	if selector.Empty() {
		return selectorGrant{}, fmt.Errorf("namespace policy %d: ownNamespaceSelector must name at least one requirement", i)
	}
	return selectorGrant{selector: selector, grant: g}, nil
}

// newGrant builds the grant of policy i. Each policy keeps its own grant.
// Merging them into one would AND the selectors of separate policies together,
// and would let a narrow policy take away the namespace a broad one granted.
func newGrant(i int, s *secretSelector) (grant, error) {
	g := grant{labels: map[string]string{}}
	for k, v := range s.labels() {
		// A malformed label can never match, so it would narrow the grant to
		// nothing and look like the policy was simply ignored. Fail loading
		// instead.
		if len(validation.IsQualifiedName(k)) != 0 {
			return grant{}, fmt.Errorf("namespace policy %d: invalid label key %q", i, k)
		}
		if len(validation.IsValidLabelValue(v)) != 0 {
			return grant{}, fmt.Errorf("namespace policy %d: invalid label value %q for key %q", i, v, k)
		}
		g.labels[k] = v
	}
	return g, nil
}

// namespaceSyncTimeout bounds the first list of namespaces, so a provider that
// cannot list them, for want of RBAC say, exits instead of never serving.
const namespaceSyncTimeout = time.Minute

// WatchNamespaces starts the namespace cache ownNamespaceSelector policies are
// matched against, for as long as ctx lives, and waits for its first list.
// It does nothing when no policy selects namespaces, so a policy file without
// one needs no access to namespaces.
func (a *NamespaceAuthorizer) WatchNamespaces(ctx context.Context, client kubernetes.Interface) error {
	if len(a.ownNamespace) == 0 {
		return nil
	}
	factory := informers.NewSharedInformerFactory(client, 0)
	namespaces := factory.Core().V1().Namespaces()
	informer := namespaces.Informer()
	factory.Start(ctx.Done())
	syncCtx, cancel := context.WithTimeout(ctx, namespaceSyncTimeout)
	defer cancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), informer.HasSynced) {
		return fmt.Errorf("namespace cache did not sync within %s", namespaceSyncTimeout)
	}
	a.namespaces = namespaces.Lister()
	return nil
}

// grantsFor returns the grants atespace holds in namespace: those the policy
// lists and, for the namespace of the atespace's own name, those of the
// ownNamespaceSelector policies it matches now. The cache holds the labels of
// its last watch event: while it cannot watch, a label change is not seen.
func (a *NamespaceAuthorizer) grantsFor(atespace, namespace string) []grant {
	// Clipped so the append below allocates instead of writing into the
	// policy's own slice, which concurrent requests share.
	grants := slices.Clip(a.allowed[atespace][namespace])
	if atespace == namespace {
		grants = append(grants, a.matching(namespace)...)
	}
	return grants
}

// matching returns the grants of the ownNamespaceSelector policies the
// namespace's labels match now. A namespace the cache does not hold matches
// none.
func (a *NamespaceAuthorizer) matching(namespace string) []grant {
	if a.namespaces == nil || len(a.ownNamespace) == 0 {
		return nil
	}
	ns, err := a.namespaces.Get(namespace)
	if err != nil {
		return nil
	}
	set := labels.Set(ns.GetLabels())
	var grants []grant
	for _, sg := range a.ownNamespace {
		if sg.selector.Matches(set) {
			grants = append(grants, sg.grant)
		}
	}
	return grants
}

// Grants returns the policy in force as atespace → sorted namespaces,
// including each namespace a namespace selector matches now.
func (a *NamespaceAuthorizer) Grants() map[string][]string {
	if a == nil {
		return nil
	}
	sets := make(map[string]map[string]bool, len(a.allowed))
	add := func(atespace, namespace string) {
		if sets[atespace] == nil {
			sets[atespace] = make(map[string]bool)
		}
		sets[atespace][namespace] = true
	}
	for atespace, namespaces := range a.allowed {
		for ns := range namespaces {
			add(atespace, ns)
		}
	}
	if a.namespaces != nil {
		all, _ := a.namespaces.List(labels.Everything())
		for _, ns := range all {
			if len(a.matching(ns.Name)) > 0 {
				add(ns.Name, ns.Name)
			}
		}
	}
	out := make(map[string][]string, len(sets))
	for atespace, namespaces := range sets {
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
	return len(a.grantsFor(atespace, namespace)) > 0
}

// AllowedSecret reports whether atespace may resolve a Secret with these
// labels in namespace. A grant without a label selector admits every Secret,
// including one that does not exist: pass nil labels to ask that question
// before reading anything from Kubernetes.
func (a *NamespaceAuthorizer) AllowedSecret(atespace, namespace string, labels map[string]string) bool {
	for _, g := range a.grantsFor(atespace, namespace) {
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
