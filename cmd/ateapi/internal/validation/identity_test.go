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
	"fmt"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateMintJWTRequest(t *testing.T) {
	// This test verifies validation of user input for minting a JWT.
	validReq := func(mods ...func(req *ateapipb.MintJWTRequest)) *ateapipb.MintJWTRequest {
		req := &ateapipb.MintJWTRequest{
			Audience:  []string{"aud1"},
			Atespace:  "as1",
			ActorName: "actor1",
			ActorUid:  "01234567-89ab-cdef-0123-456789abcdef",
		}
		for _, m := range mods {
			m(req)
		}
		return req
	}

	tests := []struct {
		name string
		req  *ateapipb.MintJWTRequest
		want field.ErrorList
	}{{
		"valid",
		validReq(),
		nil,
	}, {
		"missing audience",
		validReq(func(r *ateapipb.MintJWTRequest) { r.Audience = nil }),
		field.ErrorList{field.Required(field.NewPath("audience"), "")},
	}, {
		"too many audiences",
		validReq(func(r *ateapipb.MintJWTRequest) {
			r.Audience = make([]string, 17)
			for i := range r.Audience {
				r.Audience[i] = fmt.Sprintf("https://svc-%d.example.com", i)
			}
		}),
		field.ErrorList{field.TooMany(field.NewPath("audience"), 17, 16).WithOrigin("maxItems")},
	}, {
		"duplicate audience entry",
		validReq(func(r *ateapipb.MintJWTRequest) {
			r.Audience = []string{"https://a.example.com", "https://a.example.com"}
		}),
		field.ErrorList{field.Duplicate(field.NewPath("audience").Index(1), nil)},
	}, {
		"audience entry too long",
		validReq(func(r *ateapipb.MintJWTRequest) { r.Audience = []string{strings.Repeat("a", 513)} }),
		field.ErrorList{field.TooLong(field.NewPath("audience").Index(0), nil, 512).WithOrigin("maxLength")},
	}, {
		"missing atespace",
		validReq(func(r *ateapipb.MintJWTRequest) { r.Atespace = "" }),
		field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		"invalid atespace",
		validReq(func(r *ateapipb.MintJWTRequest) { r.Atespace = "AS1" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing actor_name",
		validReq(func(r *ateapipb.MintJWTRequest) { r.ActorName = "" }),
		field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		"invalid actor_name",
		validReq(func(r *ateapipb.MintJWTRequest) { r.ActorName = "invalid value" }),
		field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"unspecified actor_uid",
		validReq(func(r *ateapipb.MintJWTRequest) { r.ActorUid = "" }),
		nil,
	}, {
		"invalid actor_uid",
		validReq(func(r *ateapipb.MintJWTRequest) { r.ActorUid = "not a uid" }),
		field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateMintJWTRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateMintCertRequest(t *testing.T) {
	// This test verifies validation of user input for minting a certificate.
	validReq := func(mods ...func(req *ateapipb.MintCertRequest)) *ateapipb.MintCertRequest {
		req := &ateapipb.MintCertRequest{
			Worker:                    &ateapipb.ObjectRef{Name: "worker1"},
			CertificateSigningRequest: []byte{0x01},
			ExpectedActorUid:          "01234567-89ab-cdef-0123-456789abcdef",
			Purpose:                   ateapipb.ActorCertificatePurpose_ACTOR_CERTIFICATE_PURPOSE_ATUNNEL,
		}
		for _, m := range mods {
			m(req)
		}
		return req
	}

	tests := []struct {
		name string
		req  *ateapipb.MintCertRequest
		want field.ErrorList
	}{{
		"valid",
		validReq(),
		nil,
	}, {
		"oversized certificate_signing_request",
		validReq(func(r *ateapipb.MintCertRequest) { r.CertificateSigningRequest = make([]byte, 16385) }),
		field.ErrorList{field.TooLong(field.NewPath("certificate_signing_request"), nil, 16384)},
	}, {
		"missing worker",
		validReq(func(r *ateapipb.MintCertRequest) { r.Worker = nil }),
		field.ErrorList{field.Required(field.NewPath("worker"), "")},
	}, {
		"worker.atespace must be empty",
		validReq(func(r *ateapipb.MintCertRequest) { r.Worker.Atespace = "as1" }),
		field.ErrorList{field.Forbidden(field.NewPath("worker", "atespace"), "")},
	}, {
		"missing worker.name",
		validReq(func(r *ateapipb.MintCertRequest) { r.Worker.Name = "" }),
		field.ErrorList{field.Required(field.NewPath("worker", "name"), "")},
	}, {
		"invalid worker.name",
		validReq(func(r *ateapipb.MintCertRequest) { r.Worker.Name = "invalid value" }),
		field.ErrorList{field.Invalid(field.NewPath("worker", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing certificate_signing_request",
		validReq(func(r *ateapipb.MintCertRequest) { r.CertificateSigningRequest = nil }),
		field.ErrorList{field.Required(field.NewPath("certificate_signing_request"), "")},
	}, {
		"missing expected_actor_uid",
		validReq(func(r *ateapipb.MintCertRequest) { r.ExpectedActorUid = "" }),
		field.ErrorList{field.Required(field.NewPath("expected_actor_uid"), "")},
	}, {
		"invalid expected_actor_uid",
		validReq(func(r *ateapipb.MintCertRequest) { r.ExpectedActorUid = "not a uid" }),
		field.ErrorList{field.Invalid(field.NewPath("expected_actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		"unspecified purpose",
		validReq(func(r *ateapipb.MintCertRequest) {
			r.Purpose = ateapipb.ActorCertificatePurpose_ACTOR_CERTIFICATE_PURPOSE_UNSPECIFIED
		}),
		field.ErrorList{field.Required(field.NewPath("purpose"), "")},
	}, {
		"out-of-range purpose",
		validReq(func(r *ateapipb.MintCertRequest) { r.Purpose = ateapipb.ActorCertificatePurpose(99) }),
		field.ErrorList{field.Invalid(field.NewPath("purpose"), nil, "").WithOrigin("maximum")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateMintCertRequest(context.Background(), tt.req), tt.want)
		})
	}
}
