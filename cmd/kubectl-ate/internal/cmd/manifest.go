// Copyright 2026 The Agent Substrate Authors
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
	"bytes"
	"errors"
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	yamlv3 "gopkg.in/yaml.v3"
	"sigs.k8s.io/yaml"
)

// manifestMessages parses every YAML or JSON document in data into a fresh
// instance of msg's message type. YAML is a superset of JSON, so one decoder
// parses both. Documents are split on "---" the way kubectl splits them, and
// empty documents are skipped. Parsing is strict: unknown fields are an error.
func manifestMessages(data []byte, msg proto.Message) ([]proto.Message, error) {
	dec := yamlv3.NewDecoder(bytes.NewReader(data))
	typeName := string(msg.ProtoReflect().Descriptor().Name())
	var out []proto.Message
	for {
		var node yamlv3.Node
		if err := dec.Decode(&node); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("invalid YAML: %w", err)
		}
		if isNullDocument(&node) {
			continue
		}
		jsonDoc, err := yamlNodeToJSON(&node)
		if err != nil {
			return nil, err
		}
		m := msg.ProtoReflect().New().Interface()
		if err := protojson.Unmarshal(jsonDoc, m); err != nil {
			return nil, fmt.Errorf("invalid %s: %w", typeName, err)
		}
		out = append(out, m)
	}
	if len(out) == 0 {
		return nil, errors.New("manifest is empty")
	}
	return out, nil
}

// isNullDocument reports whether node is an empty YAML document, such as the
// one a trailing "---" opens.
func isNullDocument(node *yamlv3.Node) bool {
	return len(node.Content) == 1 && node.Content[0].Tag == "!!null"
}

// yamlNodeToJSON converts one YAML document node to its JSON form.
func yamlNodeToJSON(node *yamlv3.Node) ([]byte, error) {
	var buf bytes.Buffer
	enc := yamlv3.NewEncoder(&buf)
	if err := enc.Encode(node); err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	jsonDoc, err := yaml.YAMLToJSON(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	return jsonDoc, nil
}
