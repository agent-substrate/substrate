// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package volume

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLookupPlugin(t *testing.T) {
	type dummyPlugin struct {
		name string
	}

	ctx := context.Background()
	testErr := errors.New("driver not found")
	grpcNotFoundErr := status.Error(codes.NotFound, "plugin not found in registry")

	tests := []struct {
		name         string
		resolver     func(context.Context, string) (*dummyPlugin, error)
		volumeType   string
		wantPlugin   *dummyPlugin
		wantErr      bool
		errContains  string
		targetErr    error
		wantGRPCCode codes.Code
	}{
		{
			name: "success",
			resolver: func(ctx context.Context, name string) (*dummyPlugin, error) {
				if name == "csi.example.com" {
					return &dummyPlugin{name: name}, nil
				}
				return nil, testErr
			},
			volumeType: "csi.example.com",
			wantPlugin: &dummyPlugin{name: "csi.example.com"},
		},
		{
			name: "empty volume type",
			resolver: func(ctx context.Context, name string) (*dummyPlugin, error) {
				return &dummyPlugin{name: name}, nil
			},
			volumeType:  "",
			wantErr:     true,
			errContains: "volume type is required",
		},
		{
			name:        "nil resolver",
			resolver:    nil,
			volumeType:  "csi.example.com",
			wantErr:     true,
			errContains: "plugin resolver is required",
		},
		{
			name: "resolver error wrapped",
			resolver: func(ctx context.Context, name string) (*dummyPlugin, error) {
				return nil, testErr
			},
			volumeType:  "missing.csi",
			wantErr:     true,
			errContains: `failed to get volume plugin for "missing.csi"`,
			targetErr:   testErr,
		},
		{
			name: "grpc status code preserved through wrapping",
			resolver: func(ctx context.Context, name string) (*dummyPlugin, error) {
				return nil, grpcNotFoundErr
			},
			volumeType:   "notfound.csi",
			wantErr:      true,
			errContains:  `failed to get volume plugin for "notfound.csi"`,
			targetErr:    grpcNotFoundErr,
			wantGRPCCode: codes.NotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := LookupPlugin(ctx, tc.resolver, tc.volumeType)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LookupPlugin() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("LookupPlugin() error %q does not contain %q", err.Error(), tc.errContains)
				}
				if tc.targetErr != nil && !errors.Is(err, tc.targetErr) {
					t.Errorf("LookupPlugin() error %v does not wrap %v", err, tc.targetErr)
				}
				if tc.wantGRPCCode != codes.OK {
					if code := status.Code(err); code != tc.wantGRPCCode {
						t.Errorf("LookupPlugin() status code = %v, want %v", code, tc.wantGRPCCode)
					}
				}
				return
			}
			if got == nil || got.name != tc.wantPlugin.name {
				t.Errorf("LookupPlugin() = %v, want %v", got, tc.wantPlugin)
			}
		})
	}
}
