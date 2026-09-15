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
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/encoding/protojson"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

// egressPolicyFromManifest parses a single protojson-shaped YAML or JSON
// document into an EgressPolicy. Parsing is strict: unknown fields are an
// error.
func egressPolicyFromManifest(data []byte) (*ateapipb.EgressPolicy, error) {
	jsonData, err := manifestToJSON(data)
	if err != nil {
		return nil, err
	}
	policy := &ateapipb.EgressPolicy{}
	if err := protojson.Unmarshal(jsonData, policy); err != nil {
		return nil, fmt.Errorf("invalid EgressPolicy: %w", err)
	}
	return policy, nil
}

// manifestToJSON converts a YAML or JSON manifest to JSON. YAML is a superset
// of JSON, so one reader parses both. The manifest must hold exactly one
// non-empty document; the empty documents a leading or trailing "---" leaves
// behind are skipped.
func manifestToJSON(data []byte) ([]byte, error) {
	reader := k8syaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	var jsonData []byte
	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("invalid YAML: %w", err)
		}
		j, err := yaml.YAMLToJSON(doc)
		if err != nil {
			return nil, fmt.Errorf("invalid YAML: %w", err)
		}
		if string(j) == "null" {
			continue
		}
		if jsonData != nil {
			return nil, fmt.Errorf("manifest holds more than one document, expected one")
		}
		jsonData = j
	}
	if jsonData == nil {
		return nil, fmt.Errorf("manifest is empty")
	}
	return jsonData, nil
}

// policyName is the fixed name of an actor's egress policy.
const policyName = "default"

// fillEgressPolicyMetadata sets metadata.atespace to atespace and
// metadata.name to policyName when the manifest omits them. A manifest that
// names another atespace or name is rejected, not retargeted.
func fillEgressPolicyMetadata(policy *ateapipb.EgressPolicy, atespace string) error {
	if policy.Metadata == nil {
		policy.Metadata = &ateapipb.ResourceMetadata{}
	}
	switch policy.Metadata.Atespace {
	case "":
		policy.Metadata.Atespace = atespace
	case atespace:
	default:
		return fmt.Errorf("manifest metadata.atespace %q does not match --atespace %q", policy.Metadata.Atespace, atespace)
	}
	switch policy.Metadata.Name {
	case "":
		policy.Metadata.Name = policyName
	case policyName:
	default:
		return fmt.Errorf("manifest metadata.name %q must be %q", policy.Metadata.Name, policyName)
	}
	return nil
}
