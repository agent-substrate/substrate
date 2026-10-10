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

// Package model compiles a proto3 source file and provides the data model
// to represent metadata for the Substrate API.
package model

import (
	"context"
	"fmt"
	"strings"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// sourceFileName is an arbitrary internal label: Build only ever compiles
// one file, so its logical name doesn't matter beyond being self-consistent
// and appearing in error messages.
const sourceFileName = "source.proto"

// GlobalParent is the ResourceAnnotation parent that stands for the whole
// Substrate installation rather than another resource.
const GlobalParent = "Global"

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

	// Resource is the resource the method's (ateapi.method) annotation
	// names, or "" if the method has none. For example, "Actor" for
	// SuspendActor, or "EgressPolicy" for GetActorEgressPolicy.
	Resource string

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
	// Resource is the message's (ateapi.resource) annotation, or nil if the
	// message isn't a resource. For example, non-nil for "ateapi.Actor";
	// nil for "ateapi.ResourceMetadata".
	Resource *ResourceAnnotation
}

// ResourceAnnotation is a resource message's (ateapi.resource) annotation.
type ResourceAnnotation struct {
	// Parents lists the resource types this resource is a subresource of:
	// another resource's message name, or GlobalParent. For example,
	// ["Global", "Atespace"] for AccessPolicy; empty for the top-level
	// Actor.
	Parents []string
	// Singleton is true if each parent has at most one instance of this
	// resource. For example, true for EgressPolicy.
	Singleton bool
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
	// `repeated ExternalVolume external_volumes = 7;` on ActorStatus; false
	// for ResourceMetadata.atespace.
	Repeated bool

	// TypeDisplay is always a ready-to-render string. For example, "string"
	// for ResourceMetadata.name, "int64" for ResourceMetadata.version,
	// "repeated ExternalVolume" for ActorStatus.external_volumes,
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

// Build compiles source - a single, self-contained proto3 file's raw text -
// with source info retained, and walks the result into an API. Annotations
// are read from the generated descriptors compiled into this binary (see
// generatedOptions), so a message or method that has none, such as one in a
// test fixture, comes back unannotated.
func Build(ctx context.Context, source string) (*API, error) {
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			Accessor: protocompile.SourceAccessorFromMap(map[string]string{sourceFileName: source}),
		}),
		SourceInfoMode: protocompile.SourceInfoStandard,
	}
	files, err := compiler.Compile(ctx, sourceFileName)
	if err != nil {
		return nil, fmt.Errorf("while compiling proto source: %w", err)
	}
	if len(files) != 1 {
		return nil, fmt.Errorf("expected exactly one compiled file, got %d", len(files))
	}
	return fromDescriptor(files[0]), nil
}

// fromDescriptor walks fd into an API.
func fromDescriptor(fd protoreflect.FileDescriptor) *API {
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
			Resource:        methodResource(md),
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
		Resource:       resourceAnnotation(md),
	}
	fields := md.Fields()
	for i := range fields.Len() {
		m.Fields = append(m.Fields, buildField(fd, fields.Get(i)))
	}
	return m
}

// generatedOptions returns the options of the generated descriptor compiled
// into this binary under d's full name, or nil if there's none. Build reads
// annotations from there rather than from d: protocompile, compiling the
// annotations' own definitions from source, fills them in as dynamic
// messages that the generated ateapipb extension types can't read.
func generatedOptions(d protoreflect.Descriptor) proto.Message {
	g, err := protoregistry.GlobalFiles.FindDescriptorByName(d.FullName())
	if err != nil {
		return nil
	}
	return g.Options()
}

func methodResource(md protoreflect.MethodDescriptor) string {
	opts := generatedOptions(md)
	if opts == nil {
		return ""
	}
	return proto.GetExtension(opts, ateapipb.E_Method).(*ateapipb.MethodAnnotation).GetResource()
}

func resourceAnnotation(md protoreflect.MessageDescriptor) *ResourceAnnotation {
	opts := generatedOptions(md)
	if opts == nil || !proto.HasExtension(opts, ateapipb.E_Resource) {
		return nil
	}
	a := proto.GetExtension(opts, ateapipb.E_Resource).(*ateapipb.ResourceAnnotation)
	return &ResourceAnnotation{
		Parents:   a.GetParents(),
		Singleton: a.GetSingleton(),
	}
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

// Resource pairs a resource message with every method (across services)
// whose annotation names it, in encounter order.
type Resource struct {
	Message Message
	Methods []Method
}

// Resources groups api's methods by the resource their annotation names,
// one group per annotated message, in api.Messages order. Fails if a method
// has no annotation or names a message that isn't a resource, or if a
// resource names a parent that is neither GlobalParent nor another resource.
func Resources(api *API) ([]Resource, error) {
	var groups []Resource
	for _, m := range api.Messages {
		if m.Resource != nil {
			groups = append(groups, Resource{Message: m})
		}
	}

	// groups's length is fixed by now, so taking addresses into it is safe.
	byName := make(map[string]*Resource, len(groups))
	for i := range groups {
		byName[groups[i].Message.Name] = &groups[i]
	}

	for _, g := range groups {
		for _, parent := range g.Message.Resource.Parents {
			if _, ok := byName[parent]; !ok && parent != GlobalParent {
				return nil, fmt.Errorf("%s: parent %q is neither %q nor a resource in this API", g.Message.FullName, parent, GlobalParent)
			}
		}
	}

	for _, svc := range api.Services {
		for _, method := range svc.Methods {
			if method.Resource == "" {
				return nil, fmt.Errorf("%s.%s: no resource annotation - add `option (ateapi.method).resource = \"<Resource>\";`", svc.Name, method.Name)
			}
			g, ok := byName[method.Resource]
			if !ok {
				return nil, fmt.Errorf("%s.%s: annotated resource %q is not a resource in this API - annotate its message with (ateapi.resource)", svc.Name, method.Name, method.Resource)
			}
			g.Methods = append(g.Methods, method)
		}
	}

	return groups, nil
}
