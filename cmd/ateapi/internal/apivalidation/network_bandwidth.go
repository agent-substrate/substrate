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

package apivalidation

import (
	"context"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateCustom_NetworkBandwidth(_ context.Context, _ operation.Operation, path *field.Path, value, _ *ateapipb.NetworkBandwidth) field.ErrorList {
	rate := value.GetEgressBytesPerSecond()
	if rate > 0 && rate < 1024 {
		return field.ErrorList{field.Invalid(path.Child("egress_bytes_per_second"), rate, "nonzero rates must be at least 1024 bytes/s")}
	}
	return nil
}
