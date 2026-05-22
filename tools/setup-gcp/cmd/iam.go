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
	"slices"

	"cloud.google.com/go/iam/apiv1/iampb"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
)

func addProjectIamBinding(policy *iampb.Policy, role, member string) bool {
	for _, b := range policy.Bindings {
		// Skip if the policy has any conditions.
		if b.Condition != nil {
			continue
		}
		if b.Role == role {
			if slices.Contains(b.Members, member) {
				return false // Member already has this role
			}
			b.Members = append(b.Members, member)
			return true
		}
	}
	// Role not found, add a new binding
	policy.Bindings = append(policy.Bindings, &iampb.Binding{
		Role:    role,
		Members: []string{member},
	})
	return true
}

func grantGkeNodePermissions(ctx context.Context, env *Environment) error {
	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	resource := fmt.Sprintf("projects/%s", env.ProjectID)
	req := &iampb.GetIamPolicyRequest{
		Resource: resource,
	}

	policy, err := client.GetIamPolicy(ctx, req)
	if err != nil {
		return fmt.Errorf("get project iam policy: %w", err)
	}

	// TODO(#76): Use a least-privileged node service account instead.
	member := fmt.Sprintf("serviceAccount:%s-compute@developer.gserviceaccount.com", env.ProjectNumber)

	// TODO: Don't grant these permissions at project level.
	changed1 := addProjectIamBinding(policy, "roles/storage.objectViewer", member)
	changed2 := addProjectIamBinding(policy, "roles/artifactregistry.reader", member)

	if !changed1 && !changed2 {
		slog.Info("IAM policy already has required GKE node permissions. Skipping update.", slog.String("project", env.ProjectID))
		return nil
	}

	slog.Info("Setting IAM policy (grant gke node permissions)...", slog.String("project", env.ProjectID))
	setReq := &iampb.SetIamPolicyRequest{
		Resource: resource,
		Policy:   policy,
	}
	_, err = client.SetIamPolicy(ctx, setReq)
	if err != nil {
		return fmt.Errorf("set project iam policy: %w", err)
	}

	return nil
}
func grantAteletPermissions(ctx context.Context, env *Environment) error {
	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	resource := fmt.Sprintf("projects/%s", env.ProjectID)
	req := &iampb.GetIamPolicyRequest{
		Resource: resource,
	}

	policy, err := client.GetIamPolicy(ctx, req)
	if err != nil {
		return fmt.Errorf("get project iam policy: %w", err)
	}

	member := fmt.Sprintf("principal://iam.googleapis.com/projects/%s/locations/global/workloadIdentityPools/%s.svc.id.goog/subject/ns/ate-system/sa/atelet", env.ProjectNumber, env.ProjectID)

	// TODO: This shouldn't be a project-level binding.  We should grant atelet
	// access only to the specific bucket and artifact registry repository in
	// use.
	changed1 := addProjectIamBinding(policy, "roles/storage.objectAdmin", member)
	changed2 := addProjectIamBinding(policy, "roles/artifactregistry.reader", member)

	if !changed1 && !changed2 {
		slog.Info("IAM policy already has required GKE node permissions. Skipping update.", slog.String("project", env.ProjectID))
		return nil
	}

	slog.Info("Setting IAM policy (grant api server permissions)...", slog.String("project", env.ProjectID))
	setReq := &iampb.SetIamPolicyRequest{
		Resource: resource,
		Policy:   policy,
	}
	_, err = client.SetIamPolicy(ctx, setReq)
	if err != nil {
		return fmt.Errorf("set project iam policy: %w", err)
	}

	return nil
}
