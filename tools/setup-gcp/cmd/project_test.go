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

package cmd

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
)

func TestResolveProject(t *testing.T) {
	origGetProject := getProjectFn
	defer func() { getProjectFn = origGetProject }()

	tests := []struct {
		name          string
		cfg           Config
		mockProject   *resourcemanagerpb.Project
		mockErr       error
		wantProjectID string
		wantProjectNr string
		wantErr       string
		wantCalled    bool
		wantName      string
	}{
		{
			name:    "missing project ID",
			cfg:     Config{},
			wantErr: "--project-id is required",
		},
		{
			name: "only project number provided",
			cfg: Config{
				ProjectNumber: "987654",
			},
			wantErr: "--project-id is required",
		},
		{
			name: "both provided",
			cfg: Config{
				ProjectID:     "my-proj",
				ProjectNumber: "123456",
			},
			wantProjectID: "my-proj",
			wantProjectNr: "123456",
			wantCalled:    false,
		},
		{
			name: "only project ID provided resolves project number",
			cfg: Config{
				ProjectID: "my-proj",
			},
			mockProject: &resourcemanagerpb.Project{
				Name:      "projects/987654",
				ProjectId: "my-proj",
			},
			wantProjectID: "my-proj",
			wantProjectNr: "987654",
			wantCalled:    true,
			wantName:      "projects/my-proj",
		},
		{
			name: "projects prefix trimmed",
			cfg: Config{
				ProjectID: "projects/my-proj",
			},
			mockProject: &resourcemanagerpb.Project{
				Name:      "projects/987654",
				ProjectId: "my-proj",
			},
			wantProjectID: "my-proj",
			wantProjectNr: "987654",
			wantCalled:    true,
			wantName:      "projects/my-proj",
		},
		{
			name: "resolution API error",
			cfg: Config{
				ProjectID: "nonexistent-proj",
			},
			mockErr:    errors.New("project not found"),
			wantErr:    "resolve project info for projects/nonexistent-proj: project not found",
			wantCalled: true,
			wantName:   "projects/nonexistent-proj",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			var calledName string
			getProjectFn = func(ctx context.Context, name string) (*resourcemanagerpb.Project, error) {
				called = true
				calledName = name
				if tt.mockErr != nil {
					return nil, tt.mockErr
				}
				return tt.mockProject, nil
			}

			cfgCopy := tt.cfg
			err := resolveProject(t.Context(), &cfgCopy)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("got error %q, want containing %q", err.Error(), tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if called != tt.wantCalled {
				t.Errorf("getProjectFn called = %v; want %v", called, tt.wantCalled)
			}
			if tt.wantCalled && calledName != tt.wantName {
				t.Errorf("getProjectFn called with name = %q; want %q", calledName, tt.wantName)
			}
			if cfgCopy.ProjectID != tt.wantProjectID {
				t.Errorf("cfg.ProjectID = %q; want %q", cfgCopy.ProjectID, tt.wantProjectID)
			}
			if cfgCopy.ProjectNumber != tt.wantProjectNr {
				t.Errorf("cfg.ProjectNumber = %q; want %q", cfgCopy.ProjectNumber, tt.wantProjectNr)
			}
		})
	}
}

func TestResolveProjectID(t *testing.T) {
	origGetProject := getProjectFn
	defer func() { getProjectFn = origGetProject }()

	tests := []struct {
		name          string
		cfg           Config
		mockProject   *resourcemanagerpb.Project
		mockErr       error
		wantProjectID string
		wantProjectNr string
		wantErr       string
		wantCalled    bool
		wantName      string
	}{
		{
			name:    "missing project ID",
			cfg:     Config{},
			wantErr: "--project-id is required",
		},
		{
			name: "only project number provided",
			cfg: Config{
				ProjectNumber: "987654",
			},
			wantErr: "--project-id is required",
		},
		{
			name: "both provided",
			cfg: Config{
				ProjectID:     "my-proj",
				ProjectNumber: "123456",
			},
			wantProjectID: "my-proj",
			wantProjectNr: "123456",
			wantCalled:    false,
		},
		{
			name: "only project ID provided skips API call",
			cfg: Config{
				ProjectID: "my-proj",
			},
			wantProjectID: "my-proj",
			wantCalled:    false,
		},
		{
			name: "projects prefix trimmed with only project ID",
			cfg: Config{
				ProjectID: "projects/my-proj",
			},
			wantProjectID: "my-proj",
			wantCalled:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			var calledName string
			getProjectFn = func(ctx context.Context, name string) (*resourcemanagerpb.Project, error) {
				called = true
				calledName = name
				if tt.mockErr != nil {
					return nil, tt.mockErr
				}
				return tt.mockProject, nil
			}

			cfgCopy := tt.cfg
			err := resolveProjectID(t.Context(), &cfgCopy)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("got error %q, want containing %q", err.Error(), tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if called != tt.wantCalled {
				t.Errorf("getProjectFn called = %v; want %v", called, tt.wantCalled)
			}
			if tt.wantCalled && calledName != tt.wantName {
				t.Errorf("getProjectFn called with name = %q; want %q", calledName, tt.wantName)
			}
			if cfgCopy.ProjectID != tt.wantProjectID {
				t.Errorf("cfg.ProjectID = %q; want %q", cfgCopy.ProjectID, tt.wantProjectID)
			}
			if cfgCopy.ProjectNumber != tt.wantProjectNr {
				t.Errorf("cfg.ProjectNumber = %q; want %q", cfgCopy.ProjectNumber, tt.wantProjectNr)
			}
		})
	}
}
