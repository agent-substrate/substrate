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

// manifestDocument is one document of a manifest, parsed into msg's type.
type manifestDocument struct {
	// Index is the 1-based position of the document in the manifest. Empty
	// documents are counted, so the index matches what a reader sees in the
	// file even though those documents are skipped.
	Index   int
	Message proto.Message
}

// manifestMessages parses every YAML or JSON document in data into a fresh
// instance of msg's message type. YAML is a superset of JSON, so one decoder
// parses both. Documents are split on "---" the way kubectl splits them, and
// empty documents are skipped. Parsing is strict: unknown fields are an error.
//
// Errors name the document they came from once the manifest holds more than
// one, because protojson reports a flattened document's position as line 1.
func manifestMessages(data []byte, msg proto.Message) ([]manifestDocument, error) {
	nodes, err := decodeManifestDocuments(data)
	if err != nil {
		return nil, err
	}
	// Count the documents that hold a message, so a trailing "---" does not
	// make a single-document manifest look like a multi-document one.
	total := 0
	for _, node := range nodes {
		if !isNullDocument(node) {
			total++
		}
	}
	typeName := string(msg.ProtoReflect().Descriptor().Name())
	var docs []manifestDocument
	for i, node := range nodes {
		if isNullDocument(node) {
			continue
		}
		if kind := documentKind(node); kind != yamlv3.MappingNode {
			return nil, documentError(total, i+1,
				fmt.Errorf("expected a YAML mapping or a JSON object, got %s", kindName(kind)))
		}
		jsonDoc, err := yamlNodeToJSON(node)
		if err != nil {
			return nil, documentError(total, i+1, err)
		}
		m := msg.ProtoReflect().New().Interface()
		if err := protojson.Unmarshal(jsonDoc, m); err != nil {
			return nil, documentError(total, i+1, fmt.Errorf("invalid %s: %w", typeName, err))
		}
		docs = append(docs, manifestDocument{Index: i + 1, Message: m})
	}
	if len(docs) == 0 {
		return nil, errors.New("manifest is empty")
	}
	return docs, nil
}

// decodeManifestDocuments splits data into its YAML documents, keeping each
// document's position so errors can point at it.
func decodeManifestDocuments(data []byte) ([]*yamlv3.Node, error) {
	dec := yamlv3.NewDecoder(bytes.NewReader(data))
	var nodes []*yamlv3.Node
	for {
		node := new(yamlv3.Node)
		if err := dec.Decode(node); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// A syntax error after a complete document belongs to the document
			// that failed to start.
			if n := len(nodes); n > 0 {
				return nil, fmt.Errorf("document %d: invalid YAML: %w", n+1, err)
			}
			return nil, fmt.Errorf("invalid YAML: %w", err)
		}
		nodes = append(nodes, node)
	}
	if len(nodes) == 0 {
		return nil, errors.New("manifest is empty")
	}
	return nodes, nil
}

// documentError names the document an error came from, once the manifest holds
// more than one.
func documentError(total, index int, err error) error {
	if total > 1 {
		return fmt.Errorf("document %d: %w", index, err)
	}
	return err
}

// documentKind returns the kind of a document's root node.
func documentKind(node *yamlv3.Node) yamlv3.Kind {
	if node.Kind == yamlv3.DocumentNode && len(node.Content) == 1 {
		return node.Content[0].Kind
	}
	return node.Kind
}

// kindName describes a YAML node kind in an error message.
func kindName(kind yamlv3.Kind) string {
	switch kind {
	case yamlv3.DocumentNode:
		return "a document"
	case yamlv3.SequenceNode:
		return "a sequence"
	case yamlv3.MappingNode:
		return "a mapping"
	case yamlv3.ScalarNode:
		return "a scalar"
	case yamlv3.AliasNode:
		return "an alias"
	default:
		return "an unknown node"
	}
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
