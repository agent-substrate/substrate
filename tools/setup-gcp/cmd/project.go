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
	"fmt"
	"log/slog"
	"strings"

	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
)

// getProjectFn fetches a GCP project by resource name (e.g. "projects/<id>" or "projects/<number>").
// Swappable in tests.
var getProjectFn = func(ctx context.Context, name string) (*resourcemanagerpb.Project, error) {
	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create resourcemanager client: %w", err)
	}
	defer client.Close()
	return client.GetProject(ctx, &resourcemanagerpb.GetProjectRequest{Name: name})
}

// resolveProjectID ensures ProjectID is provided on cfg.
func resolveProjectID(ctx context.Context, cfg *Config) error {
	cfg.ProjectID = strings.TrimPrefix(cfg.ProjectID, "projects/")
	cfg.ProjectNumber = strings.TrimPrefix(cfg.ProjectNumber, "projects/")

	if cfg.ProjectID == "" {
		return errors.New("--project-id is required")
	}
	return nil
}

// resolveProject ensures both ProjectID and ProjectNumber are populated on cfg.
// ProjectID is required. If ProjectNumber is missing, it is resolved using the
// Google Cloud Resource Manager API.
func resolveProject(ctx context.Context, cfg *Config) error {
	if err := resolveProjectID(ctx, cfg); err != nil {
		return err
	}

	if cfg.ProjectNumber != "" {
		return nil
	}

	name := fmt.Sprintf("projects/%s", cfg.ProjectID)
	project, err := getProjectFn(ctx, name)
	if err != nil {
		return fmt.Errorf("resolve project info for %s: %w", name, err)
	}

	cfg.ProjectNumber = strings.TrimPrefix(project.GetName(), "projects/")
	slog.Info("Resolved GCP project number", slog.String("project_id", cfg.ProjectID), slog.String("project_number", cfg.ProjectNumber))
	return nil
}
