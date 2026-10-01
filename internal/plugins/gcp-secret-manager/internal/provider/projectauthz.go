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

package provider

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"

	"sigs.k8s.io/yaml"
)

// projectPolicyFile is the policy YAML: grants mapping an atespace to the
// projects whose secrets it may resolve.
type projectPolicyFile struct {
	Policies []atespaceProjectPolicy `json:"policies"`
}

type atespaceProjectPolicy struct {
	Atespace        string   `json:"atespace"`
	AllowedProjects []string `json:"allowedProjects"`
}

// ProjectAuthorizer decides which Google Cloud projects an atespace may resolve
// secrets in. It is default-deny; a nil authorizer refuses everything. Projects
// match as written: a grant by ID does not cover a URI naming it by number.
type ProjectAuthorizer struct {
	// allowed maps atespace -> set of permitted projects.
	allowed map[string]map[string]struct{}
}

// LoadProjectAuthorizer loads the YAML policy at path. Unknown fields are
// errors, so a misspelled grant fails loudly rather than granting nothing.
func LoadProjectAuthorizer(path string) (*ProjectAuthorizer, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading project policy file %q: %w", path, err)
	}
	var file projectPolicyFile
	if err := yaml.UnmarshalStrict(data, &file); err != nil {
		return nil, fmt.Errorf("parsing project policy file %q: %w", path, err)
	}
	return newProjectAuthorizer(file)
}

// newProjectAuthorizer validates file and builds an authorizer from it.
func newProjectAuthorizer(file projectPolicyFile) (*ProjectAuthorizer, error) {
	allowed := make(map[string]map[string]struct{})
	for i, p := range file.Policies {
		if p.Atespace == "" {
			return nil, fmt.Errorf("project policy %d: atespace is required", i)
		}
		set := allowed[p.Atespace]
		if set == nil {
			set = make(map[string]struct{})
			allowed[p.Atespace] = set
		}
		for _, project := range p.AllowedProjects {
			if project == "" || strings.Contains(project, "/") {
				return nil, fmt.Errorf("project policy %d: allowed project %q must be a project ID or number", i, project)
			}
			set[project] = struct{}{}
		}
	}
	return &ProjectAuthorizer{allowed: allowed}, nil
}

// Allowed reports whether atespace may resolve secrets in project.
func (a *ProjectAuthorizer) Allowed(atespace, project string) bool {
	if a == nil {
		return false
	}
	_, ok := a.allowed[atespace][project]
	return ok
}

// actorTrustDomain is the trust domain of the actor SPIFFE IDs substrate mints.
const actorTrustDomain = "substrate-actor.local"

// dns1123Label matches a substrate resource name, atespaces included.
var dns1123Label = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

// actorAtespace returns the atespace of an actor SPIFFE ID,
// spiffe://substrate-actor.local/actor/<atespace>/<name>. It follows substrate's
// internal/resources.ActorRefFromActorSPIFFEID, which this module cannot import.
func actorAtespace(id string) (string, error) {
	const format = "spiffe://" + actorTrustDomain + "/actor/<atespace>/<name>"
	u, err := url.Parse(id)
	if err != nil {
		return "", fmt.Errorf("invalid actor SPIFFE ID %q: %w", id, err)
	}
	if u.Scheme != "spiffe" || u.Host != actorTrustDomain || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("%q does not have format %s", id, format)
	}
	segments := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(segments) != 3 || segments[0] != "actor" {
		return "", fmt.Errorf("%q does not have format %s", id, format)
	}
	atespace, name := segments[1], segments[2]
	if !dns1123Label.MatchString(atespace) {
		return "", fmt.Errorf("%q is not a valid atespace", atespace)
	}
	if !dns1123Label.MatchString(name) {
		return "", fmt.Errorf("%q is not a valid actor name", name)
	}
	return atespace, nil
}
