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

package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	serviceusage "cloud.google.com/go/serviceusage/apiv1"
	"cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
)

func enableRequiredAPIs(ctx context.Context, cfg *Config) error {
	suClient, err := serviceusage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer suClient.Close()

	services := []string{
		"cloudresourcemanager.googleapis.com",
		"container.googleapis.com",
		"networkconnectivity.googleapis.com",
		"serviceusage.googleapis.com",
		"storage.googleapis.com",
		"logging.googleapis.com",
		"monitoring.googleapis.com",
		"cloudtrace.googleapis.com",
		"telemetry.googleapis.com",
	}

	slog.Info("Batch enabling services", slog.String("services", strings.Join(services, ", ")))

	req := &serviceusagepb.BatchEnableServicesRequest{
		Parent:     fmt.Sprintf("projects/%s", cfg.ProjectID),
		ServiceIds: services,
	}

	op, err := suClient.BatchEnableServices(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to start batch enabling services: %w", err)
	}

	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("failed to complete batch enabling services: %w", err)
	}

	slog.Info("Successfully enabled required services.")
	return nil
}
