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

package controlapi

import (
	"maps"
	"slices"

	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// mutableFields maps the field paths a client may set in an update_mask to its update function.
// Every other field is either output-only (server-managed), immutable or
// unsupported (e.g. '*'), and setting one is an error.
type mutableFields[T any] map[string]func(dst, src T)

// applyUpdateMask copies the masked fields from src onto dst. Fields set on src
// but absent from the mask are ignored, and a masked field that is unset on src
// is cleared on dst. Paths that set no mutable field are skipped.
func applyUpdateMask[T any](dst, src T, mask *fieldmaskpb.FieldMask, fields mutableFields[T]) {
	for _, path := range mask.GetPaths() {
		if apply, ok := fields[path]; ok {
			apply(dst, src)
		}
	}
}

// updateMaskPath is the update_mask request field that's required for
// update requests.
var updateMaskPath = field.NewPath("update_mask")

// validateUpdateMask checks that mask sets at least one field and that every
// path it sets is mutable.
func validateUpdateMask[T any](mask *fieldmaskpb.FieldMask, fields mutableFields[T]) field.ErrorList {
	paths := mask.GetPaths()
	if len(paths) == 0 {
		return field.ErrorList{field.Required(updateMaskPath, "must name at least one field to update")}
	}
	var errs field.ErrorList
	supported := slices.Sorted(maps.Keys(fields))
	for _, path := range paths {
		if _, ok := fields[path]; !ok {
			errs = append(errs, field.NotSupported(updateMaskPath, path, supported))
		}
	}
	return errs
}
