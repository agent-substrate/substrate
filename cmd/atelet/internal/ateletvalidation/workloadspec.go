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

// Custom validations for the WorkloadSpec tree. These mirror the control
// plane's rules for the ateapipb counterparts (see controlapi): values
// arriving here already passed them at template creation, so a failure is an
// internal inconsistency, not a user error.

package ateletvalidation

import (
	"context"
	"fmt"
	"strings"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/distribution/reference"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// validatePinnedImage requires a well-formed OCI image reference pinned by
// digest (e.g. "name@sha256:..."): changing the image content under a fixed
// reference invalidates snapshots. It parses with the same grammar the
// container runtimes use, so a malformed digest is rejected rather than
// treated as pinned.
func validatePinnedImage(fldPath *field.Path, value string) field.ErrorList {
	if value == "" {
		return nil // required is enforced by tags
	}
	ref, err := reference.ParseNormalizedNamed(value)
	if err != nil {
		return field.ErrorList{field.Invalid(fldPath, value, fmt.Sprintf("must be a well-formed image reference: %v", err))}
	}
	if _, ok := ref.(reference.Digested); !ok {
		return field.ErrorList{field.Invalid(fldPath, value, "must be pinned by digest (changing the image invalidates snapshots)")}
	}
	return nil
}

func ValidateCustom_ImageVolumeSource_Reference(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return validatePinnedImage(fldPath, *value)
}

// ValidateCustom_ExternalVolumeSource_StorageVolumeId rejects control
// characters (U+0000-U+0008, U+000B, U+000C, U+000E-U+001F, U+007F-U+009F).
func ValidateCustom_ExternalVolumeSource_StorageVolumeId(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if *value == "" {
		return nil // required is enforced by tags
	}
	for _, r := range *value {
		if (r >= 0x0000 && r <= 0x0008) ||
			r == 0x000B ||
			r == 0x000C ||
			(r >= 0x000E && r <= 0x001F) ||
			(r >= 0x007F && r <= 0x009F) {
			return field.ErrorList{field.Invalid(fldPath, *value, "must not contain control characters (U+0000-U+0008, U+000B, U+000C, U+000E-U+001F, U+007F-U+009F)")}
		}
	}
	return nil
}

// ValidateCustom_ExternalVolumeSource_VolumeType allows an optional
// "substrate.io/" prefix, followed by a valid DNS-1123 subdomain.
func ValidateCustom_ExternalVolumeSource_VolumeType(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if *value == "" {
		return nil
	}
	var errs field.ErrorList
	for _, msg := range validation.IsDNS1123Subdomain(strings.TrimPrefix(*value, "substrate.io/")) {
		errs = append(errs, field.Invalid(fldPath, *value, msg))
	}
	return errs
}

// ValidateCustom_SystemInfoVolume_DataSources requires every projected file
// path to be unique across all data sources: atelet writes them in order
// into one tree, so a repeated path silently clobbers the earlier file.
func ValidateCustom_SystemInfoVolume_DataSources(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []*ateletpb.SystemInfoDataSource) field.ErrorList {
	var errs field.ErrorList
	seen := sets.New[string]()
	for i, ds := range value {
		switch {
		case ds == nil:
		case ds.TrustBundle != nil:
			if seen.Has(ds.TrustBundle.Path) {
				errs = append(errs, field.Duplicate(fldPath.Index(i).Child("trust_bundle", "path"), ds.TrustBundle.Path))
			}
			seen.Insert(ds.TrustBundle.Path)
		case ds.ActorMetadata != nil:
			for j, item := range ds.ActorMetadata.Items {
				if item == nil {
					continue
				}
				if seen.Has(item.Path) {
					errs = append(errs, field.Duplicate(fldPath.Index(i).Child("actor_metadata", "items").Index(j).Child("path"), item.Path))
				}
				seen.Insert(item.Path)
			}
		}
	}
	return errs
}
