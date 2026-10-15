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

// Package gcsboundary turns actor grants into the Credential Access Boundary
// rules a downscoped GCS token carries. One rule per grant, each limited to its
// bucket and an object-name prefix.
package gcsboundary

import (
	"fmt"
	"strings"

	"golang.org/x/oauth2/google/downscope"

	"go.ate.dev/substrate/internal/objectstore"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/resources"
)

const (
	roleObjectViewer = "inRole:roles/storage.objectViewer"
	// objectAdmin, not objectCreator: an upload overwrites on retry and deletes
	// parts after a compose.
	roleObjectAdmin = "inRole:roles/storage.objectAdmin"
)

// FromGrants builds the rules. Each grant's prefix must be a gs:// prefix. The
// condition's path ends in "/", so a match cannot spill into a sibling whose
// name merely starts the same way. No objectListPrefix term is added: the node
// plugin never lists objects. downscope.NewTokenSource enforces the rule
// count, between one and ten, see
// https://docs.cloud.google.com/iam/docs/downscoping-short-lived-credentials.
func FromGrants(grants []actorscope.Grant) ([]downscope.AccessBoundaryRule, error) {
	var rules []downscope.AccessBoundaryRule
	for _, g := range grants {
		bucket, path, err := splitGCS(g.Prefix)
		if err != nil {
			return nil, err
		}
		var role string
		switch g.Access {
		case actorscope.ReadOnly:
			role = roleObjectViewer
		case actorscope.ReadWrite:
			role = roleObjectAdmin
		default:
			return nil, fmt.Errorf("grant on %s has unknown access %v", g.Prefix, g.Access)
		}
		rules = append(rules, downscope.AccessBoundaryRule{
			AvailableResource:    "//storage.googleapis.com/projects/_/buckets/" + bucket,
			AvailablePermissions: []string{role},
			Condition: &downscope.AvailabilityCondition{
				Expression: fmt.Sprintf("resource.name.startsWith('projects/_/buckets/%s/objects/%s')", celEscape(bucket), celEscape(path)),
			},
		})
	}
	return rules, nil
}

// splitGCS returns the bucket and object path of a gs:// prefix, the path with
// its trailing slash.
func splitGCS(prefix resources.StoragePrefix) (bucket, path string, err error) {
	if !strings.HasPrefix(prefix.String(), "gs://") {
		return "", "", fmt.Errorf("prefix %q is not a gs:// prefix", prefix)
	}
	return objectstore.BucketPrefix(prefix)
}

// celEscaper quotes the two characters that would end or alter a CEL
// single-quoted string literal. A storage location may contain a quote, since
// Substrate accepts one, so it is escaped rather than refused.
var celEscaper = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

func celEscape(s string) string { return celEscaper.Replace(s) }
