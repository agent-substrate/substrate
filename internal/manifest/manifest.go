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

// Package manifest parses manifests, in their protojson form, into protobuf
// messages. A manifest is either a JSON file, which holds one record, or a YAML
// file, which holds any number of documents delimited by the YAML document
// separator ("---"). JSON has no multi-document form, so several JSON objects
// separated by "---" are a YAML file, not a JSON one.
package manifest

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

// message constrains PM to a pointer to the proto message type M.
type message[M any] interface {
	*M
	proto.Message
}

// Parse parses each record of a manifest into an M, in order. Each non-empty
// YAML document is a record, and a JSON file is a single record. Empty
// documents are skipped, as kubectl does. Parsing is strict: unknown fields are
// an error, so typos don't silently drop configuration.
func Parse[M any, PM message[M]](data []byte) ([]PM, error) {
	docs, err := splitDocuments(data)
	if err != nil {
		return nil, err
	}
	var msgs []PM
	for i, doc := range docs {
		if isEmpty(doc) {
			continue
		}
		msg, err := unmarshal[M, PM](doc)
		if err != nil {
			if len(docs) > 1 {
				err = fmt.Errorf("document %d: %w", i+1, err)
			}
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	if len(msgs) == 0 {
		return nil, errEmpty
	}
	return msgs, nil
}

// One returns the only record of msgs, as returned by Parse, for callers that
// accept a single record. It is an error if there is more than one.
func One[M any, PM message[M]](msgs []PM) (PM, error) {
	switch len(msgs) {
	case 0:
		return nil, errEmpty
	case 1:
		return msgs[0], nil
	}
	return nil, fmt.Errorf("manifest holds %d %s records, expected one", len(msgs), msgs[0].ProtoReflect().Descriptor().Name())
}

var errEmpty = errors.New("manifest is empty")

// splitDocuments returns the JSON form of each YAML document in data, and
// errEmpty when there are none. YAML is a superset of JSON, so one decoder
// parses both.
func splitDocuments(data []byte) ([][]byte, error) {
	dec := yamlv3.NewDecoder(bytes.NewReader(data))
	var docs [][]byte
	for {
		var node yamlv3.Node
		if err := dec.Decode(&node); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("invalid YAML: %w", err)
		}
		// sigs.k8s.io/yaml converts YAML to JSON the way Kubernetes does, but
		// reads only the first document, so hand it one document at a time.
		single, err := yamlv3.Marshal(&node)
		if err != nil {
			return nil, fmt.Errorf("invalid YAML: %w", err)
		}
		doc, err := yaml.YAMLToJSON(single)
		if err != nil {
			return nil, fmt.Errorf("invalid YAML: %w", err)
		}
		docs = append(docs, doc)
	}
	if len(docs) == 0 {
		return nil, errEmpty
	}
	return docs, nil
}

func isEmpty(doc []byte) bool { return string(doc) == "null" }

func unmarshal[M any, PM message[M]](doc []byte) (PM, error) {
	msg := PM(new(M))
	if err := protojson.Unmarshal(doc, msg); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", msg.ProtoReflect().Descriptor().Name(), err)
	}
	return msg, nil
}
