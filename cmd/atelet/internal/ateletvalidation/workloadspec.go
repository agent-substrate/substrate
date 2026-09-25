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
	"regexp"
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

// envEntryNameRE constrains env var names to any printable ASCII character
// except '='.
var envEntryNameRE = regexp.MustCompile(`^[ -<>-~]+$`)

func ValidateCustom_EnvEntry_Name(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if *value == "" {
		return nil // required is enforced by tags
	}
	if !envEntryNameRE.MatchString(*value) {
		return field.ErrorList{field.Invalid(fldPath, *value, "may contain any printable ASCII character except '='")}
	}
	return nil
}

// mountPathBadSegmentRE matches '.' or '..' path segments.
var mountPathBadSegmentRE = regexp.MustCompile(`(^|/)[.][.]?(/|$)`)

// ValidateCustom_VolumeMount_MountPath requires a clean absolute Unix path
// that starts with '/', is not '/', and contains no ':', '.' or '..'
// segments, '//', trailing '/', or control characters.
func ValidateCustom_VolumeMount_MountPath(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	p := *value
	if p == "" {
		return nil // required is enforced by tags
	}
	bad := !strings.HasPrefix(p, "/") || len(p) == 1 ||
		strings.HasSuffix(p, "/") || strings.Contains(p, "//") ||
		strings.Contains(p, ":") || mountPathBadSegmentRE.MatchString(p)
	if !bad {
		for _, r := range p {
			if r < 0x20 || r == 0x7f {
				bad = true
				break
			}
		}
	}
	if bad {
		return field.ErrorList{field.Invalid(fldPath, p, "must be a clean absolute Unix path: must start with '/', not be '/', and contain no ':', '..', '.', '//', trailing '/', or control characters")}
	}
	return nil
}

// ValidateCustom_Container_VolumeMounts rejects nested mounts (volumes cannot
// mount onto other volumes). Mount-path uniqueness is enforced by the list
// key.
func ValidateCustom_Container_VolumeMounts(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []*ateletpb.VolumeMount) field.ErrorList {
	var errs field.ErrorList
	for i, m := range value {
		path := m.GetMountPath()
		if path == "" {
			continue // required is enforced by tags
		}
		for j := 0; j < i; j++ {
			prior := value[j].GetMountPath()
			if prior == "" || prior == path {
				continue
			}
			if strings.HasPrefix(path, prior+"/") || strings.HasPrefix(prior, path+"/") {
				errs = append(errs, field.Invalid(fldPath.Index(i).Child("mount_path"), path,
					fmt.Sprintf("must not nest under or over another mount (%q)", prior)))
			}
		}
	}
	return errs
}

// capabilityRE constrains Linux capability names: uppercase, without the
// "CAP_" prefix (which is added when the OCI spec is written; the prefixed
// spelling would silently grant nothing).
var capabilityRE = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

func validateCapabilities(fldPath *field.Path, caps []string, allowAll bool) field.ErrorList {
	var errs field.ErrorList
	for i, c := range caps {
		p := fldPath.Index(i)
		switch {
		case c == "ALL" && !allowAll:
			errs = append(errs, field.Invalid(p, c, "add does not accept 'ALL'; name the individual capabilities the container needs"))
		case c == "ALL":
		case len(c) > 63:
			errs = append(errs, field.TooLong(p, nil, 63))
		case strings.HasPrefix(c, "CAP_"):
			errs = append(errs, field.Invalid(p, c, "must be named without the 'CAP_' prefix (e.g. 'NET_BIND_SERVICE')"))
		case !capabilityRE.MatchString(c):
			errs = append(errs, field.Invalid(p, c, "must be an uppercase capability name like 'NET_BIND_SERVICE'"))
		}
	}
	return errs
}

func ValidateCustom_Capabilities_Add(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []string) field.ErrorList {
	return validateCapabilities(fldPath, value, false)
}

func ValidateCustom_Capabilities_Drop(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []string) field.ErrorList {
	return validateCapabilities(fldPath, value, true)
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
