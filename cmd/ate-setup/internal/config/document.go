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

package config

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/internal/version"
)

// Outcome is how the run that produced a document ended.
type Outcome string

const (
	Succeeded Outcome = "succeeded"
	Failed    Outcome = "failed"
)

// documentMetadataKey is the document's one reserved top-level key. ParseFile
// skips it the way it skips apiVersion and kind, which is what lets a document
// this package wrote be handed straight back to --config.
const documentMetadataKey = "cluster"

// DocumentMetadata records which run produced a document. It is written by
// ate-setup and ignored when the document is read back as configuration.
type DocumentMetadata struct {
	// Context is the cluster the run targeted, and names the file.
	Context string `json:"context,omitempty"`
	// Outcome is whether the run completed.
	Outcome Outcome `json:"outcome,omitempty"`
	// FailedAt is the command that returned an error, for a failed run.
	FailedAt string `json:"failedAt,omitempty"`
	// WrittenAt and WrittenBy identify the run for a reader comparing two
	// documents.
	WrittenAt string `json:"writtenAt,omitempty"`
	WrittenBy string `json:"writtenBy,omitempty"`
}

// Document is a resolved configuration as a file. The same shape serves the
// record of a run and the --config input of the next one, so retrying takes no
// conversion step.
type Document struct {
	Metadata DocumentMetadata
	// values holds the settings by canonical key, as Resolve saw them.
	values map[string]Value
}

// NewDocument records the settings a channel supplied.
//
// Settings still on their declared default are omitted. An unset setting has
// to keep tracking its default when a later release changes it, and writing
// the default out would freeze the value this run happened to get.
//
// Secret settings are omitted whatever supplied them, and their keys are
// returned so the caller can say which a retry must re-supply. Without that,
// the omission is discovered only when the retry fails.
func NewDocument(r *Resolved, meta DocumentMetadata) (*Document, []string) {
	meta.WrittenAt = time.Now().UTC().Format(time.RFC3339)
	meta.WrittenBy = "ate-setup " + version.String()

	d := &Document{Metadata: meta, values: map[string]Value{}}
	var omitted []string
	for _, v := range r.Origins() { // Supplied values only
		if v.Setting.Secret {
			omitted = append(omitted, v.Setting.Key)
			continue
		}
		d.values[v.Setting.Key] = v
	}
	sort.Strings(omitted)
	return d, omitted
}

// Keys returns the settings the document records, sorted.
func (d *Document) Keys() []string {
	out := make([]string, 0, len(d.values))
	for k := range d.values {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Marshal renders the document, nesting the dotted keys so the result is
// ordinary --config input rather than a second format.
//
// Scalars are written in their declared kind: a count reads as 7, not "7".
// ParseFile accepts either, but a document an operator edits should look like
// the example in the documentation.
func (d *Document) Marshal() ([]byte, error) {
	doc := map[string]any{
		"apiVersion":        FileAPIVersion,
		"kind":              FileKind,
		documentMetadataKey: d.Metadata,
	}
	for _, key := range d.Keys() {
		nest(doc, strings.Split(key, "."), scalar(d.values[key]))
	}
	return yaml.Marshal(doc)
}

// scalar renders a value in its declared kind, falling back to the raw string
// when it does not parse. Resolve rejected any such value long before this, so
// the fallback is for a Document assembled by hand in a test.
func scalar(v Value) any {
	switch v.Setting.Kind {
	case KindBool:
		b, err := strconv.ParseBool(v.Raw)
		if err != nil {
			return v.Raw
		}
		return b
	case KindInt:
		n, err := strconv.Atoi(v.Raw)
		if err != nil {
			return v.Raw
		}
		return n
	default:
		return v.Raw
	}
}

// nest walks path into doc, creating the intermediate maps. A key cannot both
// hold a value and have children -- the registry has no such pair, and
// TestRegistryKeysDoNotNest keeps it that way -- so a collision would be a
// programming error, and the leaf simply wins.
func nest(doc map[string]any, path []string, value any) {
	for _, step := range path[:len(path)-1] {
		child, ok := doc[step].(map[string]any)
		if !ok {
			child = map[string]any{}
			doc[step] = child
		}
		doc = child
	}
	doc[path[len(path)-1]] = value
}
