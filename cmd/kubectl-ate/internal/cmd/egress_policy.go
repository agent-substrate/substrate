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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"
)

// policyName is the fixed name of an actor's egress policy.
const policyName = "default"

// egressPolicyFromManifest parses a single protojson-shaped YAML or JSON
// document. Parsing is strict: unknown fields are an error. metadata.atespace
// and metadata.name default to atespace and policyName.
func egressPolicyFromManifest(data []byte, atespace string) (*ateapipb.EgressPolicy, error) {
	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	if string(jsonData) == "null" {
		return nil, fmt.Errorf("manifest is empty")
	}
	policy := &ateapipb.EgressPolicy{}
	if err := protojson.Unmarshal(jsonData, policy); err != nil {
		return nil, err
	}
	if policy.Metadata == nil {
		policy.Metadata = &ateapipb.ResourceMetadata{}
	}
	switch policy.Metadata.Atespace {
	case "":
		policy.Metadata.Atespace = atespace
	case atespace:
	default:
		return nil, fmt.Errorf("manifest metadata.atespace %q does not match --atespace %q", policy.Metadata.Atespace, atespace)
	}
	switch policy.Metadata.Name {
	case "":
		policy.Metadata.Name = policyName
	case policyName:
	default:
		return nil, fmt.Errorf("manifest metadata.name %q must be %q", policy.Metadata.Name, policyName)
	}
	return policy, nil
}
