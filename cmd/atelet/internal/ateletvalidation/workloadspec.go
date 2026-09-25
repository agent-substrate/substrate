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

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

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
