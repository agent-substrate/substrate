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

package validation

import (
	"context"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateMintJWTRequest(ctx context.Context, req *ateapipb.MintJWTRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_MintJWTRequest(ctx, op, nil, req, nil)
}

func ValidateMintCertRequest(ctx context.Context, req *ateapipb.MintCertRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_MintCertRequest(ctx, op, nil, req, nil)
}

// maxCSRBytes bounds MintCertRequest's CSR. Real CSRs are a few KB; this is
// a guardrail, applied here because maxLength does not support bytes fields.
const maxCSRBytes = 16384

func ValidateCustom_MintCertRequest_CertificateSigningRequest(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []byte) field.ErrorList {
	if len(value) > maxCSRBytes {
		return field.ErrorList{field.TooLong(fldPath, nil, maxCSRBytes)}
	}
	return nil
}
