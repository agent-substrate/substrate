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

// Package fieldmask applies protobuf FieldMask-based updates to proto messages.
package fieldmask

import (
	"maps"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// MutableFields is the set of exact field paths a client may name in an
// update_mask. Every path must be listed in full (e.g. "nested.value"), and
// only exactly-listed paths are accepted: naming a message-typed field does
// not implicitly permit paths nested under it, and vice versa. Every other
// field is either output-only (server-managed), immutable or unsupported
// (e.g. '*'), and setting one is an error.
type MutableFields map[string]struct{}

// NewMutableFields returns the MutableFields set containing exactly the given paths.
func NewMutableFields(paths ...string) MutableFields {
	fields := make(MutableFields, len(paths))
	for _, p := range paths {
		fields[p] = struct{}{}
	}
	return fields
}

// Validate checks that mask sets at least one field and that every path it
// sets is exactly listed in fields.
func Validate(mask *fieldmaskpb.FieldMask, fields MutableFields, fldPath *field.Path) field.ErrorList {
	paths := mask.GetPaths()
	if len(paths) == 0 {
		return field.ErrorList{field.Required(fldPath, "must name at least one field to update")}
	}
	var errs field.ErrorList
	supported := slices.Sorted(maps.Keys(fields))
	for _, path := range paths {
		if _, ok := fields[path]; !ok {
			errs = append(errs, field.NotSupported(fldPath, path, supported))
		}
	}
	return errs
}

// Apply copies the fields addressed by mask from src onto dst. A masked
// field that is unset on src is cleared on dst, and a masked nested field
// initializes any unset intermediate message on dst as needed. Every path is
// expected to have already been checked against the resource's
// MutableFields (Validate); a path that doesn't resolve to a real field is
// silently skipped rather than treated as a caller error.
//
// dst never ends up aliasing src so mutating dst or the original src
// afterward cannot affect the other.
func Apply[T proto.Message](dst, src T, mask *fieldmaskpb.FieldMask) {
	dstMsg, srcMsg := dst.ProtoReflect(), proto.Clone(src).ProtoReflect()
	for _, path := range mask.GetPaths() {
		copyMaskedField(dstMsg, srcMsg, strings.Split(path, "."))
	}
}

func copyMaskedField(dst, src protoreflect.Message, segments []string) {
	fd := dst.Descriptor().Fields().ByName(protoreflect.Name(segments[0]))
	if fd == nil {
		return
	}
	if len(segments) == 1 {
		// TODO: when fd is message-kind, this copies it whole, including any
		// output-only or immutable subfields it may have. Skip those once
		// fields carry that annotation.
		if src.Has(fd) {
			dst.Set(fd, src.Get(fd))
		} else {
			dst.Clear(fd)
		}
		return
	}
	if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() {
		return
	}
	if !src.Has(fd) && !dst.Has(fd) {
		return
	}
	copyMaskedField(dst.Mutable(fd).Message(), src.Get(fd).Message(), segments[1:])
}
