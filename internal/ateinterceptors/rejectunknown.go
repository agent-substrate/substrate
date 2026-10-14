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
	"github.com/agent-substrate/substrate/internal/removedfields"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/reflect/protoreflect"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// removedFieldLogged holds the principal and field pairs already logged, so
// each client is named once per process and not on every request.
var removedFieldLogged sync.Map

// RejectUnknownFieldsUnaryInterceptor refuses any request carrying a field
// this binary has no descriptor for, at any level of nesting. A field the API
// removed (package removedfields) is accepted and logged instead.
func RejectUnknownFieldsUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if msg, ok := req.(proto.Message); ok {
		errs, removed := findUnknownFields(msg)
		if len(errs) > 0 {
			return nil, apierror.InvalidArgument("%v", errs.ToAggregate())
		}
		for _, path := range removed {
			logRemovedField(ctx, info.FullMethod, path)
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

// findUnknownFields reports an error for every message in m, at every level of
// nesting, that carries an unknown field other than a removed one, and
// returns the path of each removed field it found. An unknown field on m
// itself is reported at "request".
func findUnknownFields(m proto.Message) (field.ErrorList, []*field.Path) {
	r := m.ProtoReflect()
	if !r.IsValid() {
		return nil, nil
	}

	var errs field.ErrorList
	var removed []*field.Path
	if err := protorange.Range(r, func(p protopath.Values) error {
		msg, ok := p.Index(-1).Value.Interface().(protoreflect.Message)
		if !ok || len(msg.GetUnknown()) == 0 {
			return nil
		}
		path := toFieldPath(p.Path)
		num, found, tolerated := firstRefusedField(msg.Descriptor().FullName(), msg.GetUnknown())
		for _, f := range tolerated {
			removed = append(removed, childPath(path, string(f.Name)))
		}
		if !found {
			return nil
		}
		if path == nil {
			// Use "request" for the root message which has no field path.
			path = field.NewPath("request")
		}
		detail := "unknown field"
		if num > 0 {
			detail = "unknown field with protobuf tag " + strconv.Itoa(int(num))
		}
		errs = append(errs, field.Invalid(path, field.OmitValueType{}, detail))
		return nil
	}); err != nil {
		errs = append(errs, field.InternalError(field.NewPath("request"), err))
	}
	return errs, removed
}

// firstRefusedField walks the unknown fields b of the message msg and returns
// the number of the first one that is not a removed field (0 when b is
// malformed there), whether there is one, and the removed fields b carries.
func firstRefusedField(msg protoreflect.FullName, b protoreflect.RawFields) (protowire.Number, bool, []removedfields.Field) {
	var tolerated []removedfields.Field
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return 0, true, tolerated
		}
		vn := protowire.ConsumeFieldValue(num, typ, b[n:])
		if vn < 0 {
			return num, true, tolerated
		}
		f, ok := removedfields.Lookup(msg, num, typ)
		if !ok {
			return num, true, tolerated
		}
		tolerated = append(tolerated, f)
		b = b[n+vn:]
	}
	return 0, false, tolerated
}

// childPath is p.Child(name), with a nil p (the root message) read as the
// request itself.
func childPath(p *field.Path, name string) *field.Path {
	if p == nil {
		return field.NewPath("request", name)
	}
	return p.Child(name)
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
