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

package agentsession

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Script is a loaded agent-session script: a named step sequence plus the
// actor memory it needs.
//
// The YAML form is a direct transcription of the Go types:
//
//	name: coding-session
//	min_actor_memory: 1Gi
//	steps:
//	- name: 01_read_task
//	  agent: Boots, reads the task prompt, loads its context window
//	  think: 2s
//	  ops:
//	  - fill_ram: {key: agent_context, size: 32Mi}
//	  - ping: {}
//
// Each op is a one-key map whose key is the op kind and whose value holds
// that kind's arguments. Sizes are Kubernetes quantities (16Mi, 64Ki);
// think times are Go durations (2s, 1.5s). Unknown kinds, unknown fields,
// and arguments a kind does not take are errors.
type Script struct {
	Name string
	// MinActorMemory is the smallest actor memory limit the script is known
	// to run under, in bytes. The driver refuses to start against a smaller
	// template. Required, and at least the script's declared RAM plus disk
	// (the data dir is tmpfs).
	MinActorMemory int64
	Steps          []Step
}

// scriptDoc is the YAML shape of a Script.
type scriptDoc struct {
	Name           string    `yaml:"name"`
	MinActorMemory string    `yaml:"min_actor_memory"`
	Steps          []stepDoc `yaml:"steps"`
}

type stepDoc struct {
	Name  string  `yaml:"name"`
	Agent string  `yaml:"agent"`
	Think string  `yaml:"think"`
	Ops   []opDoc `yaml:"ops"`
}

// opDoc is one op: exactly one entry, kind name to arguments.
type opDoc map[string]opArgs

type opArgs struct {
	Key      string `yaml:"key,omitempty"`
	Size     string `yaml:"size,omitempty"`
	Millis   int64  `yaml:"millis,omitempty"`
	Parallel int32  `yaml:"parallel,omitempty"`
}

// opSpec says which arguments an op kind takes.
type opSpec struct {
	kind     opKind
	key      bool // takes key
	size     bool // takes size
	millis   bool // takes millis
	parallel bool // takes parallel (implies millis)
}

var opSpecs = map[string]opSpec{
	"ingest":           {kind: opIngest, key: true, size: true},
	"burn_cpu":         {kind: opBurnCPU, millis: true, parallel: true},
	"write_disk":       {kind: opWriteDisk, key: true, size: true},
	"read_disk_digest": {kind: opReadDiskDigest, key: true},
	"read_disk_data":   {kind: opReadDiskData, key: true},
	"fill_ram":         {kind: opFillRAM, key: true, size: true},
	"churn_ram":        {kind: opChurnRAM, key: true, size: true},
	"walk_ram":         {kind: opWalkRAM, key: true},
	"ping":             {kind: opPing},
	"dwell":            {kind: opDwell, millis: true},
}

// opNames is the inverse of opSpecs, for encoding and messages.
var opNames = func() map[opKind]string {
	m := make(map[opKind]string, len(opSpecs))
	for name, spec := range opSpecs {
		m[spec.kind] = name
	}
	return m
}()

// keyRE mirrors glutton's diskKeyRE: keys name files under the actor's data
// dir, so nothing that could escape it is accepted.
var keyRE = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// scriptNameRE bounds script names, which double as the --agentsession-script
// knob value and the embedded file name.
var scriptNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// Decode parses a YAML script and validates it.
func Decode(data []byte) (*Script, error) {
	var doc scriptDoc
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("parse script: %w", err)
	}
	s := &Script{Name: doc.Name}
	if doc.MinActorMemory != "" {
		n, err := parseSize(doc.MinActorMemory)
		if err != nil {
			return nil, fmt.Errorf("min_actor_memory: %w", err)
		}
		s.MinActorMemory = n
	}
	for i, sd := range doc.Steps {
		step, err := decodeStep(sd)
		if err != nil {
			return nil, fmt.Errorf("step %d (%q): %w", i+1, sd.Name, err)
		}
		s.Steps = append(s.Steps, step)
	}
	if err := Validate(s); err != nil {
		return nil, err
	}
	return s, nil
}

func decodeStep(sd stepDoc) (Step, error) {
	step := Step{Name: sd.Name, Agent: sd.Agent}
	if sd.Think != "" {
		d, err := time.ParseDuration(sd.Think)
		if err != nil {
			return step, fmt.Errorf("think: %w", err)
		}
		step.Think = d
	}
	for i, od := range sd.Ops {
		o, err := decodeOp(od)
		if err != nil {
			return step, fmt.Errorf("op %d: %w", i+1, err)
		}
		step.Ops = append(step.Ops, o)
	}
	return step, nil
}

func decodeOp(od opDoc) (op, error) {
	if len(od) != 1 {
		return op{}, fmt.Errorf("an op is a single-key map, got %d keys", len(od))
	}
	var name string
	var args opArgs
	for name, args = range od {
	}
	spec, ok := opSpecs[name]
	if !ok {
		return op{}, fmt.Errorf("unknown op kind %q", name)
	}
	o := op{kind: spec.kind}
	switch {
	case spec.key && args.Key == "":
		return op{}, fmt.Errorf("%s: key is required", name)
	case !spec.key && args.Key != "":
		return op{}, fmt.Errorf("%s: takes no key", name)
	}
	if args.Key != "" {
		if !keyRE.MatchString(args.Key) {
			return op{}, fmt.Errorf("%s: key %q must match %s", name, args.Key, keyRE)
		}
		o.key = args.Key
	}
	switch {
	case spec.size && args.Size == "":
		return op{}, fmt.Errorf("%s: size is required", name)
	case !spec.size && args.Size != "":
		return op{}, fmt.Errorf("%s: takes no size", name)
	}
	if args.Size != "" {
		n, err := parseSize(args.Size)
		if err != nil {
			return op{}, fmt.Errorf("%s: size: %w", name, err)
		}
		o.bytes = n
	}
	if spec.millis {
		if args.Millis <= 0 {
			return op{}, fmt.Errorf("%s: millis must be positive", name)
		}
		o.millis = args.Millis
	} else if args.Millis != 0 {
		return op{}, fmt.Errorf("%s: takes no millis", name)
	}
	if spec.parallel {
		if args.Parallel < 0 {
			return op{}, fmt.Errorf("%s: parallel cannot be negative", name)
		}
		o.parallel = max(args.Parallel, 1)
	} else if args.Parallel != 0 {
		return op{}, fmt.Errorf("%s: takes no parallel", name)
	}
	return o, nil
}

// parseSize reads a Kubernetes quantity as a positive byte count.
func parseSize(s string) (int64, error) {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0, err
	}
	n, ok := q.AsInt64()
	if !ok || n <= 0 {
		return 0, fmt.Errorf("%q is not a positive whole byte count", s)
	}
	return n, nil
}

// FormatSize renders a byte count as a Kubernetes quantity (32Mi, 1Gi).
func FormatSize(n int64) string {
	return resource.NewQuantity(n, resource.BinarySI).String()
}

// Encode renders a Script as YAML in the form Decode reads, with one
// flow-style line per op so the file reads like the step table it is.
func Encode(s *Script) ([]byte, error) {
	root := &yaml.Node{Kind: yaml.MappingNode}
	addScalar(root, "name", s.Name)
	addScalar(root, "min_actor_memory", FormatSize(s.MinActorMemory))
	steps := &yaml.Node{Kind: yaml.SequenceNode}
	for _, st := range s.Steps {
		sn := &yaml.Node{Kind: yaml.MappingNode}
		addScalar(sn, "name", st.Name)
		addScalar(sn, "agent", st.Agent)
		addScalar(sn, "think", st.Think.String())
		ops := &yaml.Node{Kind: yaml.SequenceNode}
		for _, o := range st.Ops {
			on, err := encodeOp(o)
			if err != nil {
				return nil, fmt.Errorf("step %q: %w", st.Name, err)
			}
			ops.Content = append(ops.Content, on)
		}
		sn.Content = append(sn.Content, scalar("ops"), ops)
		steps.Content = append(steps.Content, sn)
	}
	root.Content = append(root.Content, scalar("steps"), steps)

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(root); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeOp(o op) (*yaml.Node, error) {
	name, ok := opNames[o.kind]
	if !ok {
		return nil, fmt.Errorf("unknown op kind %d", o.kind)
	}
	spec := opSpecs[name]
	args := &yaml.Node{Kind: yaml.MappingNode, Style: yaml.FlowStyle}
	if spec.key {
		addScalar(args, "key", o.key)
	}
	if spec.size {
		addScalar(args, "size", FormatSize(o.bytes))
	}
	if spec.millis {
		addScalar(args, "millis", strconv.FormatInt(o.millis, 10))
	}
	if spec.parallel {
		addScalar(args, "parallel", strconv.Itoa(int(o.parallel)))
	}
	wrapper := &yaml.Node{Kind: yaml.MappingNode}
	wrapper.Content = append(wrapper.Content, scalar(name), args)
	return wrapper, nil
}

func scalar(v string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Value: v}
}

func addScalar(m *yaml.Node, k, v string) {
	m.Content = append(m.Content, scalar(k), scalar(v))
}
