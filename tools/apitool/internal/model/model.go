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

// Package model provides the data model to represent metadata for the Substrate API.
package model

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// API is the whole documented surface of one proto file.
type API struct {
	// Services lists every RPC service declared in the file. For example,
	// ateapi.proto's single "Control" service.
	Services []Service
	// Messages lists every message declared in the file, top-level and
	// nested, flattened into one slice. For example, "ateapi.Actor" and
	// "ateapi.ResourceMetadata" both appear here as top-level entries.
	Messages []Message
	// Enums lists every enum declared in the file, top-level and nested,
	// flattened the same way as Messages. For example, both the top-level
	// "ateapi.ActorState" and the nested "ateapi.ExternalVolume.Status"
	// appear here.
	Enums []Enum
}

// MessagesByFullName indexes a.Messages by their FullName, for looking up
// a Method's InputName/OutputName or a Field's TypeFullName.
func (a API) MessagesByFullName() map[string]Message {
	byName := make(map[string]Message, len(a.Messages))
	for _, m := range a.Messages {
		byName[m.FullName] = m
	}
	return byName
}

// Service is one RPC service. For example, ateapi.proto's "Control" service.
type Service struct {
	// FullName is the service's proto full name. For example, "ateapi.Control".
	FullName string
	// Name is the service's short name. For example, "Control".
	Name string
	// Comment is the service's leading doc comment. For example, "Control
	// is the primary API for Agent Substrate." Empty if fd wasn't built
	// with comments - see Build.
	Comment string
	// Methods lists every RPC the service declares, in declaration order.
	// For example, Control's GetActor, CreateActor, UpdateActor, ...
	Methods []Method
}

// Method is one RPC declared on a Service.
type Method struct {
	// Name is the RPC's name. For example, "CreateActor".
	Name string
	// Comment is the method's leading doc comment. For example, "Create a
	// new Actor deriving from a given ActorTemplate."
	Comment string
	// InputName is the full name of the request message, for linking. For
	// example, "ateapi.CreateActorRequest" for CreateActor.
	InputName string
	// OutputName is the full name of the response message, for linking.
	// For example, "ateapi.Actor" for CreateActor - most standard methods
	// return the resource itself rather than a wrapper message.
	OutputName string

	// ServiceFullName is the declaring service's full name. For example,
	// "ateapi.Control".
	ServiceFullName string
	// ServiceName is the declaring service's short name. For example, "Control".
	ServiceName string
}

// Message is one message declared in the file, top-level or nested.
type Message struct {
	// FullName is the message's proto full name. For example,
	// "ateapi.ResourceMetadata".
	FullName string
	// Name is the message's short name, with its enclosing message's name
	// dotted in front for a nested type. For example, "ResourceMetadata"
	// for a top-level message. ateapi.proto has no nested message today
	// (only a nested enum, ExternalVolume.Status - see Enum.Name for that
	// shape); a nested message named "Child" inside "Parent" would read
	// "Parent.Child" here.
	Name string
	// ParentFullName is the enclosing message's full name, or "" for a
	// top-level message. For example, "" for "ateapi.ResourceMetadata".
	ParentFullName string
	// Comment is the message's leading doc comment. For example,
	// "ResourceMetadata holds the common fields carried by every Substrate
	// resource."
	Comment string
	// Fields lists the message's fields, in declaration order. For
	// example, ResourceMetadata's atespace, name, uid, version,
	// create_time, update_time.
	Fields []Field
}

// FieldsByType returns every field of m whose type is typeFullName, in
// declaration order - the caller decides what to do with zero, one, or
// more than one match.
func (m Message) FieldsByType(typeFullName string) []Field {
	var fields []Field
	for _, f := range m.Fields {
		if f.TypeKind == "message" && f.TypeFullName == typeFullName {
			fields = append(fields, f)
		}
	}
	return fields
}

// Field is one field declared on a Message.
type Field struct {
	// Name is the field's proto name. For example, "atespace" for
	// ResourceMetadata.atespace.
	Name string
	// Number is the field's proto field number. For example, 4 for
	// ResourceMetadata.version (`int64 version = 4;`).
	Number int32
	// Comment is the field's leading doc comment. For example, "version is
	// increased on every mutation." for ResourceMetadata.version.
	Comment string

	// Repeated is true for a `repeated` field, not a map - see
	// MapValueKind for those. For example, true for
	// `repeated ExternalVolume actor_volumes = 7;` on ActorStatus; false
	// for ResourceMetadata.atespace.
	Repeated bool

	// TypeDisplay is always a ready-to-render string. For example, "string"
	// for ResourceMetadata.name, "int64" for ResourceMetadata.version,
	// "repeated ExternalVolume" for ActorStatus.actor_volumes,
	// "map<string, ArchAssets>" for SandboxAssets.assets, "ActorState" for
	// ActorStatus.state, or "google.protobuf.Timestamp" for
	// ResourceMetadata.create_time.
	TypeDisplay string
	// TypeFullName and TypeKind ("message" or "enum") are set only for a
	// singular or repeated message/enum field. Empty for scalars and maps -
	// see MapValueFullName/MapValueKind for a map's value type. For
	// example, for Actor.metadata (`ResourceMetadata metadata = 1;`),
	// TypeFullName is "ateapi.ResourceMetadata" and TypeKind is "message";
	// for ActorStatus.state (`ActorState state = 1;`), TypeFullName is
	// "ateapi.ActorState" and TypeKind is "enum".
	TypeFullName string
	TypeKind     string
	// TypeIsExternal is true when the type isn't declared in this proto
	// file (for example, an imported well-known type). For example, true
	// for ResourceMetadata.create_time (google.protobuf.Timestamp,
	// imported from google/protobuf/timestamp.proto); false for
	// Actor.metadata (ateapi.ResourceMetadata, declared in ateapi.proto
	// itself).
	TypeIsExternal bool

	// MapValueFullName and MapValueKind mirror TypeFullName/TypeKind, but
	// for a map field's value type. For example, for SandboxAssets.assets
	// (`map<string, ArchAssets> assets = 2;`), MapValueFullName is
	// "ateapi.ArchAssets" and MapValueKind is "message"; for
	// Selector.match_labels (`map<string, string> match_labels = 1;`),
	// both are empty, since the map's value is a scalar.
	MapValueFullName string
	MapValueKind     string

	// OneofName is set only for a member of a real, explicit `oneof`
	// group. ateapi.proto has none today - the API forbids real oneofs by
	// convention (see lint.NoOneofs) - so OneofName is always "" for every
	// field this model currently produces.
	OneofName string
	// Proto3Optional is true for a proto3 `optional` scalar - protoreflect
	// models it as a synthetic oneof, not a real one. ateapi.proto
	// declares no `optional` field today, so this is always false for
	// every field this model currently produces.
	Proto3Optional bool
}

// Enum is one enum declared in the file, top-level or nested.
type Enum struct {
	// FullName is the enum's proto full name. For example, "ateapi.ActorState"
	// for a top-level enum, or "ateapi.ExternalVolume.Status" for one
	// nested inside the ExternalVolume message.
	FullName string
	// Name is the enum's short name, with its enclosing message's name
	// dotted in front for a nested enum. For example, "ActorState" for
	// the top-level enum, or "ExternalVolume.Status" for the one nested
	// inside ExternalVolume.
	Name string
	// ParentFullName is the enclosing message's full name, or "" for a
	// top-level enum. For example, "" for "ateapi.ActorState", or
	// "ateapi.ExternalVolume" for "ateapi.ExternalVolume.Status".
	ParentFullName string
	// Comment is the enum's leading doc comment. ateapi.proto's ActorState
	// and ExternalVolume.Status have none today, so this is often "" in
	// practice even though the field is populated the same way as for a
	// message or field.
	Comment string
	// Values lists the enum's values, in declaration order. For example,
	// ExternalVolume.Status's STATUS_UNSPECIFIED, STATUS_PENDING,
	// STATUS_CREATED, STATUS_DELETING.
	Values []EnumValue
}

// ValueByNumber returns e's value numbered number, or nil if none has that
// number.
func (e Enum) ValueByNumber(number int32) *EnumValue {
	for i := range e.Values {
		if e.Values[i].Number == number {
			return &e.Values[i]
		}
	}
	return nil
}

// EnumValue is one value declared on an Enum.
type EnumValue struct {
	// Name is the value's proto name. For example, "STATUS_PENDING" for
	// ExternalVolume.Status.
	Name string
	// Number is the value's proto number. For example, 1 for
	// `STATUS_PENDING = 1;`.
	Number int32
	// Comment is the value's leading doc comment. For example, "Volume
	// creation pending in the storage system." for ExternalVolume.Status's
	// STATUS_PENDING.
	Comment string
}

// Build walks fd into an API. Comments are populated only if fd came from
// parser.BuildFileDescriptor.
func Build(fd protoreflect.FileDescriptor) *API {
	api := &API{}

	services := fd.Services()
	for i := range services.Len() {
		api.Services = append(api.Services, buildService(fd, services.Get(i)))
	}

	collectMessages(fd, fd.Messages(), api)
	collectEnums(fd, fd.Enums(), api)

	return api
}

// ScopeToService returns a copy of api with only the service named
// serviceName, plus every message/enum reachable from its methods'
// input/output types (including through map values). Empty result if
// serviceName doesn't exist.
func ScopeToService(api *API, serviceName string) *API {
	var svc *Service
	for i := range api.Services {
		if api.Services[i].Name == serviceName {
			svc = &api.Services[i]
			break
		}
	}
	if svc == nil {
		return &API{}
	}

	messagesByName := make(map[string]*Message, len(api.Messages))
	for i := range api.Messages {
		messagesByName[api.Messages[i].FullName] = &api.Messages[i]
	}

	reachableMessages := map[string]bool{}
	reachableEnums := map[string]bool{}

	var visitMessage func(name string)
	visitMessage = func(name string) {
		if reachableMessages[name] {
			return
		}
		msg, ok := messagesByName[name]
		if !ok {
			return
		}
		reachableMessages[name] = true
		for _, f := range msg.Fields {
			switch f.TypeKind {
			case "message":
				visitMessage(f.TypeFullName)
			case "enum":
				reachableEnums[f.TypeFullName] = true
			}
			switch f.MapValueKind {
			case "message":
				visitMessage(f.MapValueFullName)
			case "enum":
				reachableEnums[f.MapValueFullName] = true
			}
		}
	}

	for _, m := range svc.Methods {
		visitMessage(m.InputName)
		visitMessage(m.OutputName)
	}

	out := &API{Services: []Service{*svc}}
	for i := range api.Messages {
		if reachableMessages[api.Messages[i].FullName] {
			out.Messages = append(out.Messages, api.Messages[i])
		}
	}
	for i := range api.Enums {
		if reachableEnums[api.Enums[i].FullName] {
			out.Enums = append(out.Enums, api.Enums[i])
		}
	}
	return out
}

func collectMessages(fd protoreflect.FileDescriptor, mds protoreflect.MessageDescriptors, api *API) {
	for i := range mds.Len() {
		md := mds.Get(i)
		if md.IsMapEntry() {
			continue // synthetic per-map-field type, never a documented message.
		}
		api.Messages = append(api.Messages, buildMessage(fd, md))
		collectMessages(fd, md.Messages(), api)
		collectEnums(fd, md.Enums(), api)
	}
}

func collectEnums(fd protoreflect.FileDescriptor, eds protoreflect.EnumDescriptors, api *API) {
	for i := range eds.Len() {
		api.Enums = append(api.Enums, buildEnum(fd, eds.Get(i)))
	}
}

func buildService(fd protoreflect.FileDescriptor, sd protoreflect.ServiceDescriptor) Service {
	svc := Service{
		FullName: string(sd.FullName()),
		Name:     string(sd.Name()),
		Comment:  comment(fd, sd),
	}
	methods := sd.Methods()
	for i := range methods.Len() {
		md := methods.Get(i)
		svc.Methods = append(svc.Methods, Method{
			Name:            string(md.Name()),
			Comment:         comment(fd, md),
			InputName:       string(md.Input().FullName()),
			OutputName:      string(md.Output().FullName()),
			ServiceFullName: svc.FullName,
			ServiceName:     svc.Name,
		})
	}
	return svc
}

func buildMessage(fd protoreflect.FileDescriptor, md protoreflect.MessageDescriptor) Message {
	m := Message{
		FullName:       string(md.FullName()),
		Name:           shortName(fd, md.FullName()),
		ParentFullName: string(parentFullName(md)),
		Comment:        comment(fd, md),
	}
	fields := md.Fields()
	for i := range fields.Len() {
		m.Fields = append(m.Fields, buildField(fd, fields.Get(i)))
	}
	return m
}

func buildField(fd protoreflect.FileDescriptor, field protoreflect.FieldDescriptor) Field {
	f := Field{
		Name:     string(field.Name()),
		Number:   int32(field.Number()),
		Comment:  comment(fd, field),
		Repeated: field.IsList(),
	}

	switch {
	case field.IsMap():
		f.TypeDisplay = fmt.Sprintf("map<%s, %s>", scalarOrTypeName(fd, field.MapKey()), scalarOrTypeName(fd, field.MapValue()))
		switch field.MapValue().Kind() {
		case protoreflect.MessageKind, protoreflect.GroupKind:
			f.MapValueFullName = string(field.MapValue().Message().FullName())
			f.MapValueKind = "message"
		case protoreflect.EnumKind:
			f.MapValueFullName = string(field.MapValue().Enum().FullName())
			f.MapValueKind = "enum"
		}
	case field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind:
		name := shortName(fd, field.Message().FullName())
		f.TypeFullName = string(field.Message().FullName())
		f.TypeKind = "message"
		f.TypeIsExternal = field.Message().ParentFile() != fd
		f.TypeDisplay = repeatedPrefix(field) + name
	case field.Kind() == protoreflect.EnumKind:
		name := shortName(fd, field.Enum().FullName())
		f.TypeFullName = string(field.Enum().FullName())
		f.TypeKind = "enum"
		f.TypeIsExternal = field.Enum().ParentFile() != fd
		f.TypeDisplay = repeatedPrefix(field) + name
	default:
		f.TypeDisplay = repeatedPrefix(field) + field.Kind().String()
	}

	if oneof := field.ContainingOneof(); oneof != nil {
		if oneof.IsSynthetic() {
			f.Proto3Optional = true
		} else {
			f.OneofName = string(oneof.Name())
		}
	}

	return f
}

func buildEnum(fd protoreflect.FileDescriptor, ed protoreflect.EnumDescriptor) Enum {
	e := Enum{
		FullName:       string(ed.FullName()),
		Name:           shortName(fd, ed.FullName()),
		ParentFullName: string(parentFullName(ed)),
		Comment:        comment(fd, ed),
	}
	values := ed.Values()
	for i := range values.Len() {
		v := values.Get(i)
		e.Values = append(e.Values, EnumValue{
			Name:    string(v.Name()),
			Number:  int32(v.Number()),
			Comment: comment(fd, v),
		})
	}
	return e
}

func repeatedPrefix(field protoreflect.FieldDescriptor) string {
	if field.IsList() {
		return "repeated "
	}
	return ""
}

// scalarOrTypeName names a map key or value field's type: the scalar kind
// name, or the message/enum's short name.
func scalarOrTypeName(fd protoreflect.FileDescriptor, mapField protoreflect.FieldDescriptor) string {
	switch mapField.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return shortName(fd, mapField.Message().FullName())
	case protoreflect.EnumKind:
		return shortName(fd, mapField.Enum().FullName())
	default:
		return mapField.Kind().String()
	}
}

// shortName strips fd's package prefix from a full name. For example,
// "ateapi.Actor.Status" -> "Actor.Status".
func shortName(fd protoreflect.FileDescriptor, full protoreflect.FullName) string {
	return strings.TrimPrefix(string(full), string(fd.Package())+".")
}

// parentFullName returns d's enclosing message's full name, or "" if d is
// declared at the top level of the file.
func parentFullName(d protoreflect.Descriptor) protoreflect.FullName {
	if md, ok := d.Parent().(protoreflect.MessageDescriptor); ok {
		return md.FullName()
	}
	return ""
}

func comment(fd protoreflect.FileDescriptor, d protoreflect.Descriptor) string {
	return strings.TrimSpace(fd.SourceLocations().ByDescriptor(d).LeadingComments)
}

// Resource pairs a message with every method (across services) that
// resourceForMethodName matches it to, in encounter order.
type Resource struct {
	Message Message
	Methods []Method
}

// Resources partitions api's methods by resource, matching each method's
// name against resourceNames (see resourceForMethodName), in api.Messages
// order. Fails if a method matches no resource name or multiple
// equally-specific ones, or if a matched name has no corresponding message
// - there's no proto annotation backing this, so these are the only checks.
func Resources(api *API) ([]Resource, error) {
	// Pass 1: resolve every method to a resource name, and collect the
	// names actually referenced.
	methodResource := make(map[string]string, len(api.Services))
	referencedNames := map[string]bool{}
	for _, svc := range api.Services {
		for _, method := range svc.Methods {
			resource, err := resourceForMethodName(method.Name)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", svc.Name, method.Name, err)
			}
			methodResource[svc.Name+"."+method.Name] = resource
			referencedNames[resource] = true
		}
	}

	// Pass 2: one group per referenced resource, in api.Messages order,
	// deleting each name as matched so any left over names an unknown
	// resource.
	groups := make([]Resource, 0, len(referencedNames))
	for _, m := range api.Messages {
		if referencedNames[m.Name] {
			groups = append(groups, Resource{Message: m})
			delete(referencedNames, m.Name)
		}
	}
	for name := range referencedNames {
		return nil, fmt.Errorf("resource name %q, matched from a method name, has no corresponding message in this API", name)
	}

	// Pass 3: append each method to its group. groups's length is fixed by
	// now, so taking addresses into it is safe.
	byName := make(map[string]*Resource, len(groups))
	for i := range groups {
		byName[groups[i].Message.Name] = &groups[i]
	}
	for _, svc := range api.Services {
		for _, method := range svc.Methods {
			resource := methodResource[svc.Name+"."+method.Name]
			byName[resource].Methods = append(byName[resource].Methods, method)
		}
	}

	return groups, nil
}

// AttrNode is one row of a message's field list, recursively including
// child fields when the type expands. Children is nil for scalars, enums,
// and external types.
type AttrNode struct {
	Field Field
	// Path is the dotted field path from the root (for example,
	// "source_snapshot.tag.atespace"), for identifying deeply nested rows.
	Path     string
	Children []AttrNode
}

// maxAttrTreeDepth bounds recursion independent of the cycle guard below.
const maxAttrTreeDepth = 6

// BuildAttrTree turns fields into an AttrNode tree, expanding each
// message-typed field (or map value) against api's messages. Guards
// against a cyclic schema by never re-expanding a type already on the
// current path.
func BuildAttrTree(api *API, fields []Field) []AttrNode {
	messagesByName := make(map[string]*Message, len(api.Messages))
	for i := range api.Messages {
		messagesByName[api.Messages[i].FullName] = &api.Messages[i]
	}
	return buildAttrNodes(messagesByName, fields, "", map[string]bool{}, 0)
}

func buildAttrNodes(byName map[string]*Message, fields []Field, pathPrefix string, ancestors map[string]bool, depth int) []AttrNode {
	nodes := make([]AttrNode, len(fields))
	for i, f := range fields {
		path := f.Name
		if pathPrefix != "" {
			path = pathPrefix + "." + f.Name
		}
		nodes[i] = AttrNode{Field: f, Path: path}

		var childTypeName string
		switch {
		case f.TypeKind == "message":
			childTypeName = f.TypeFullName
		case f.MapValueKind == "message":
			childTypeName = f.MapValueFullName
		default:
			continue
		}
		if depth >= maxAttrTreeDepth || ancestors[childTypeName] {
			continue
		}
		child, ok := byName[childTypeName]
		if !ok {
			continue // external/well-known type: not expandable.
		}

		nextAncestors := make(map[string]bool, len(ancestors)+1)
		for k := range ancestors {
			nextAncestors[k] = true
		}
		nextAncestors[childTypeName] = true
		nodes[i].Children = buildAttrNodes(byName, child.Fields, path, nextAncestors, depth+1)
	}
	return nodes
}

var resourceNames = []string{
	"Actor",
	"ActorSnapshot",
	"ActorSnapshotTag",
	"ActorTemplate",
	"Atespace",
	"Worker",
}

func resourceForMethodName(methodName string) (string, error) {
	var matches []string
	for _, name := range resourceNames {
		if strings.Contains(methodName, name) {
			matches = append(matches, name)
		}
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("no name in resourceNames is contained in %q - add one naming the resource this method operates on", methodName)
	}

	longest := matches[:1]
	for _, m := range matches[1:] {
		switch {
		case len(m) > len(longest[0]):
			longest = []string{m}
		case len(m) == len(longest[0]):
			longest = append(longest, m)
		}
	}
	if len(longest) > 1 {
		return "", fmt.Errorf("%q matches multiple equally-specific resource names %v - rename the method or the resource so only one matches", methodName, longest)
	}
	return longest[0], nil
}
