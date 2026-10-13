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

package ateinterceptors

import (
	"context"
	"log/slog"
	"strconv"
	"sync"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/principal"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/reflect/protoreflect"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// removedField is a field the API removed that a client built against an
// older API may still send. It is matched by number and wire type, so a later
// field that reuses the number with another type is never mistaken for it.
type removedField struct {
	name     protoreflect.Name
	number   protowire.Number
	wireType protowire.Type
}

// removedFields are dropped from a request before the unknown-field check, for
// a transition: a client that still sets one keeps working, and the value is
// ignored. Every other unknown field is refused.
var removedFields = map[protoreflect.FullName][]removedField{
	// on_pause: a pause captures the template's snapshot scope, so the value
	// has nothing left to set.
	"ateapi.SnapshotConfig": {{name: "on_pause", number: 1, wireType: protowire.VarintType}},
}

// removedFieldLogged holds the principal and field pairs already logged, so
// each client is named once per process and not on every request.
var removedFieldLogged sync.Map

// RejectUnknownFieldsUnaryInterceptor refuses any request carrying a field
// this binary has no descriptor for, at any level of nesting. A field in
// removedFields is dropped and logged instead.
func RejectUnknownFieldsUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if msg, ok := req.(proto.Message); ok {
		for _, path := range dropRemovedFields(msg) {
			logRemovedField(ctx, info.FullMethod, path)
		}
		if errs := findUnknownFields(msg); len(errs) > 0 {
			return nil, apierror.InvalidArgument("%v", errs.ToAggregate())
		}
	}
	return handler(ctx, req)
}

// logRemovedField logs a removed field the caller sent, once per principal
// and field path.
func logRemovedField(ctx context.Context, method string, path *field.Path) {
	p, _ := principal.FromContext(ctx)
	if _, seen := removedFieldLogged.LoadOrStore(p.Kind+"/"+p.Issuer+"/"+p.ID+"|"+path.String(), struct{}{}); seen {
		return
	}
	slog.WarnContext(ctx, "Request carries a removed field; ignoring it",
		slog.String("field", path.String()),
		slog.String("method", method),
		slog.Any("principal", p),
	)
}

// dropRemovedFields removes every field of removedFields from the unknown
// fields of m and the messages it holds, and returns the path of each one it
// removed.
func dropRemovedFields(m proto.Message) []*field.Path {
	r := m.ProtoReflect()
	if !r.IsValid() {
		return nil
	}

	var dropped []*field.Path
	_ = protorange.Range(r, func(p protopath.Values) error {
		msg, ok := p.Index(-1).Value.Interface().(protoreflect.Message)
		if !ok || len(msg.GetUnknown()) == 0 {
			return nil
		}
		removed := removedFields[msg.Descriptor().FullName()]
		if len(removed) == 0 {
			return nil
		}
		var kept protoreflect.RawFields
		for b := msg.GetUnknown(); len(b) > 0; {
			num, typ, n := protowire.ConsumeTag(b)
			if n < 0 {
				// Malformed: keep the rest for findUnknownFields to report.
				kept = append(kept, b...)
				break
			}
			vn := protowire.ConsumeFieldValue(num, typ, b[n:])
			if vn < 0 {
				kept = append(kept, b...)
				break
			}
			if f, ok := matchRemoved(removed, num, typ); ok {
				dropped = append(dropped, childPath(toFieldPath(p.Path), string(f.name)))
			} else {
				kept = append(kept, b[:n+vn]...)
			}
			b = b[n+vn:]
		}
		msg.SetUnknown(kept)
		return nil
	})
	return dropped
}

func matchRemoved(removed []removedField, num protowire.Number, typ protowire.Type) (removedField, bool) {
	for _, f := range removed {
		if f.number == num && f.wireType == typ {
			return f, true
		}
	}
	return removedField{}, false
}

// childPath is p.Child(name), with a nil p (the root message) read as the
// request itself.
func childPath(p *field.Path, name string) *field.Path {
	if p == nil {
		return field.NewPath("request", name)
	}
	return p.Child(name)
}

// findUnknownFields reports an error for every unknown field in m, at every
// level of nesting. An unknown field on m itself is reported at "request".
func findUnknownFields(m proto.Message) field.ErrorList {
	r := m.ProtoReflect()
	if !r.IsValid() {
		return nil
	}

	var errs field.ErrorList
	if err := protorange.Range(r, func(p protopath.Values) error {
		msg, ok := p.Index(-1).Value.Interface().(protoreflect.Message)
		if !ok || len(msg.GetUnknown()) == 0 {
			return nil
		}
		path := toFieldPath(p.Path)
		if path == nil {
			// Use "request" for the root message which has no field path.
			path = field.NewPath("request")
		}
		detail := "unknown field"
		if num, _, n := protowire.ConsumeTag(msg.GetUnknown()); n > 0 {
			detail = "unknown field with protobuf tag " + strconv.Itoa(int(num))
		}
		errs = append(errs, field.Invalid(path, field.OmitValueType{}, detail))
		return nil
	}); err != nil {
		errs = append(errs, field.InternalError(field.NewPath("request"), err))
	}
	return errs
}

// toFieldPath renders a protopath as a field.Path. The root step renders as
// nil.
func toFieldPath(p protopath.Path) *field.Path {
	var out *field.Path
	for _, step := range p {
		switch step.Kind() {
		case protopath.FieldAccessStep:
			out = out.Child(step.FieldDescriptor().TextName())
		case protopath.ListIndexStep:
			out = out.Index(step.ListIndex())
		case protopath.MapIndexStep:
			out = out.Key(step.MapIndex().String())
		}
	}
	return out
}
