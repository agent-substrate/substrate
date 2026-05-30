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
	"os"
	"path/filepath"

	dashboard "cloud.google.com/go/monitoring/dashboard/apiv1"
	"cloud.google.com/go/monitoring/dashboard/apiv1/dashboardpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/encoding/protojson"
)

// dashboardsToApply lists the Cloud Monitoring dashboard JSON files (relative to
// the repo root, so run setup from the repo root) that setup creates or updates.
var dashboardsToApply = []string{
	"monitoring/dashboards/ate-grpc-dashboard.json",
	"monitoring/dashboards/ate-e2e-latency-dashboard.json",
}

// createMonitoringDashboards creates or updates each dashboard in
// dashboardsToApply. It is idempotent: dashboards are matched by displayName and
// updated in place, because CreateDashboard always creates a new dashboard (so
// calling it repeatedly would produce duplicates).
func createMonitoringDashboards(ctx context.Context, env *Environment) error {
	client, err := dashboard.NewDashboardsClient(ctx)
	if err != nil {
		return fmt.Errorf("create dashboards client: %w", err)
	}
	defer client.Close()

	parent := "projects/" + env.ProjectID

	// Index existing dashboards by displayName to decide create vs update.
	existing := map[string]*dashboardpb.Dashboard{}
	it := client.ListDashboards(ctx, &dashboardpb.ListDashboardsRequest{Parent: parent})
	for {
		d, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("list dashboards: %w", err)
		}
		existing[d.GetDisplayName()] = d
	}

	for _, path := range dashboardsToApply {
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		d := &dashboardpb.Dashboard{}
		if err := protojson.Unmarshal(data, d); err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}

		if cur, ok := existing[d.GetDisplayName()]; ok {
			// Update in place: reuse the existing resource name and etag.
			d.Name = cur.GetName()
			d.Etag = cur.GetEtag()
			slog.Info("Updating dashboard",
				slog.String("displayName", d.GetDisplayName()),
				slog.String("name", d.GetName()),
				slog.String("file", filepath.Base(path)))
			if _, err := client.UpdateDashboard(ctx, &dashboardpb.UpdateDashboardRequest{Dashboard: d}); err != nil {
				return fmt.Errorf("update dashboard %q: %w", d.GetDisplayName(), err)
			}
		} else {
			slog.Info("Creating dashboard",
				slog.String("displayName", d.GetDisplayName()),
				slog.String("file", filepath.Base(path)))
			if _, err := client.CreateDashboard(ctx, &dashboardpb.CreateDashboardRequest{Parent: parent, Dashboard: d}); err != nil {
				return fmt.Errorf("create dashboard %q: %w", d.GetDisplayName(), err)
			}
		}
	}
	return nil
}
