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

// This file is the wire side of the checkpoint: it turns a CalloutResult into
// the ProcessingResponse Envoy expects.

package inner

import (
	"context"
	"log/slog"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
)

// allowResponse passes the request upstream, carrying whatever credentials the
// decision asked to inject.
func allowResponse(ctx context.Context, decision CalloutResult) *extprocv3.ProcessingResponse {
	common := &extprocv3.CommonResponse{}

	var setHeaders []*corev3.HeaderValueOption
	// Header names are case-insensitive, so "Authorization" and
	// "authorization" are one header and the index is keyed on the folded name.
	indexByName := make(map[string]int, len(decision.Credentials))
	for _, credential := range decision.Credentials {
		if credential.Key == "" {
			continue
		}
		if strings.HasPrefix(credential.Key, ":") {
			slog.ErrorContext(ctx, "refusing to inject a credential into a pseudo-header",
				slog.String("credential_header", credential.Key))
			continue
		}

		option := &corev3.HeaderValueOption{
			// RawValue rather than Value: newer Envoy drops Value.
			Header: &corev3.HeaderValue{Key: credential.Key, RawValue: []byte(credential.Value)},
			// OVERWRITE, not the APPEND_IF_EXISTS_OR_ADD default. The actor
			// sets its own headers inside its own tunnel, so an append would
			// leave the actor's forged value beside the real one and let the
			// upstream pick.
			AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
		}

		name := strings.ToLower(credential.Key)
		if at, duplicate := indexByName[name]; duplicate {
			// Two OVERWRITEs of one header leave the last one standing, so this
			// resolves the same way Envoy would. It is logged because the
			// policy asked for two values and can only get one: silently
			// dropping a credential is how an upstream ends up rejecting a call
			// nobody can explain.
			slog.ErrorContext(ctx, "policy returned two values for one credential header; keeping the last",
				slog.String("credential_header", credential.Key))
			setHeaders[at] = option
			continue
		}
		indexByName[name] = len(setHeaders)
		setHeaders = append(setHeaders, option)
	}

	if len(setHeaders) > 0 {
		common.HeaderMutation = &extprocv3.HeaderMutation{SetHeaders: setHeaders}
	}

	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_RequestHeaders{
			RequestHeaders: &extprocv3.HeadersResponse{Response: common},
		},
	}
}

// DenyDetails is the response_code_details a denial carries. It populates
// %RESPONSE_CODE_DETAILS% in the gateway's access log, which is what makes an
// extprocd denial distinguishable from the CONNECT checkpoint's 403 and from
// the cleartext chain's direct_response — all three are otherwise just a 403
// on a request the actor believed it was allowed to make.
const DenyDetails = "extprocd_egress_policy_denied"

// denyBody is what the actor is told. Deliberately the same string for every
// denial: the reason is a property of the gateway's policy, and echoing it to
// the workload turns the deny path into a way to enumerate that policy.
const denyBody = "egress denied: destination is not permitted by egress policy\n"

// denyResponse tells Envoy to answer the request itself with a 403.
func denyResponse() *extprocv3.ProcessingResponse {
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_ImmediateResponse{
			ImmediateResponse: &extprocv3.ImmediateResponse{
				Status:  &envoy_type.HttpStatus{Code: envoy_type.StatusCode_Forbidden},
				Body:    []byte(denyBody),
				Details: DenyDetails,
				Headers: &extprocv3.HeaderMutation{
					SetHeaders: []*corev3.HeaderValueOption{{
						// RawValue rather than Value: newer Envoy drops Value.
						Header: &corev3.HeaderValue{Key: "content-type", RawValue: []byte("text/plain")},
					}},
				},
			},
		},
	}
}

// unmodifiedResponse is the do-nothing response matching one request kind, or
// nil when the kind has no counterpart to answer with.
func unmodifiedResponse(kind any) *extprocv3.ProcessingResponse {
	switch kind.(type) {
	case *extprocv3.ProcessingRequest_RequestHeaders:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestHeaders{
			RequestHeaders: &extprocv3.HeadersResponse{},
		}}
	case *extprocv3.ProcessingRequest_ResponseHeaders:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseHeaders{
			ResponseHeaders: &extprocv3.HeadersResponse{},
		}}
	case *extprocv3.ProcessingRequest_RequestBody:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestBody{
			RequestBody: &extprocv3.BodyResponse{},
		}}
	case *extprocv3.ProcessingRequest_ResponseBody:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseBody{
			ResponseBody: &extprocv3.BodyResponse{},
		}}
	case *extprocv3.ProcessingRequest_RequestTrailers:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestTrailers{
			RequestTrailers: &extprocv3.TrailersResponse{},
		}}
	case *extprocv3.ProcessingRequest_ResponseTrailers:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseTrailers{
			ResponseTrailers: &extprocv3.TrailersResponse{},
		}}
	}
	return nil
}
