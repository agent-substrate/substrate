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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	"github.com/spf13/cobra"
	iam "google.golang.org/api/iam/v1"
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

// grantGkeNodePermissions ensures the dedicated node service account exists and
// carries required project roles.
func grantGkeNodePermissions(ctx context.Context, cfg *Config) error {
	if err := ensureNodeServiceAccount(ctx, cfg); err != nil {
		return err
	}
	warnNodeImagePull(cfg)
	return nil
}

// ensureNodeServiceAccount creates the dedicated GKE node service account and
// grants it the defaultNodeServiceAccount project role.
func ensureNodeServiceAccount(ctx context.Context, cfg *Config) error {
	svc, err := iam.NewService(ctx)
	if err != nil {
		return fmt.Errorf("create iam client: %w", err)
	}
	email := nodeGSAEmail(cfg)
	resource := fmt.Sprintf("projects/%s/serviceAccounts/%s", cfg.ProjectID, email)
	_, err = svc.Projects.ServiceAccounts.Get(resource).Context(ctx).Do()
	switch {
	case err == nil:
		slog.Info("Service account exists. Skipping create.", slog.String("gsa", email))
	case isNotFound(err):
		slog.Info("Creating node service account...", slog.String("gsa", email))
		_, err = svc.Projects.ServiceAccounts.Create("projects/"+cfg.ProjectID, &iam.CreateServiceAccountRequest{
			AccountId:      nodeGSAName(cfg),
			ServiceAccount: &iam.ServiceAccount{DisplayName: "Substrate GKE nodes"},
		}).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("create service account %s: %w", email, err)
		}
	default:
		return fmt.Errorf("get service account %s: %w", email, err)
	}

	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	projectResource := fmt.Sprintf("projects/%s", cfg.ProjectID)
	member := "serviceAccount:" + email

	// A just-created service account can take a while to propagate, and granting
	// too soon fails with "does not exist". Re-reading the policy also resolves etag conflicts.
	for attempt := 1; ; attempt++ {
		policy, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: projectResource})
		if err != nil {
			return fmt.Errorf("get project iam policy: %w", err)
		}
		if !addProjectIamBinding(policy, "roles/container.defaultNodeServiceAccount", member) {
			slog.Info("IAM policy already has required GKE node permissions. Skipping update.")
			return nil
		}
		slog.Info("Setting IAM policy (grant GKE node permissions)...", slog.String("member", member))
		_, err = client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: projectResource, Policy: policy})
		if err == nil {
			return nil
		}
		if attempt >= 6 {
			return fmt.Errorf("set project iam policy: %w", err)
		}
		slog.Warn("Setting IAM policy failed, retrying...", slog.Int("attempt", attempt), slog.Any("err", err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
		}
	}
}

var gcrLocations = map[string]string{
	"gcr.io":      "us",
	"us.gcr.io":   "us",
	"eu.gcr.io":   "europe",
	"asia.gcr.io": "asia",
}

// parseArtifactRegistryRepo extracts the GCP project, location, and repository
// name from an Artifact Registry or Container Registry image repository string.
func parseArtifactRegistryRepo(imageRepo string) (project, location, repo string, ok bool) {
	parts := strings.Split(strings.TrimSpace(imageRepo), "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", "", "", false
	}
	if loc, ok := gcrLocations[parts[0]]; ok {
		return parts[1], loc, parts[0], true
	}
	if strings.HasSuffix(parts[0], "-docker.pkg.dev") {
		loc := strings.TrimSuffix(parts[0], "-docker.pkg.dev")
		if loc == "" || len(parts) < 3 || parts[2] == "" {
			return "", "", "", false
		}
		return parts[1], loc, parts[2], true
	}
	return "", "", "", false
}

func warnNodeImagePull(cfg *Config) {
	slog.Warn("Grant the node service account read access to the image repository",
		slog.String("command", imagePullGrantCommand(os.Getenv("KO_DOCKER_REPO"), nodeGSAEmail(cfg))))
}

// imagePullGrantCommand fills in what it can parse from imageRepo and leaves
// placeholders for the rest.
func imagePullGrantCommand(imageRepo, email string) string {
	project, loc, repo, ok := parseArtifactRegistryRepo(imageRepo)
	if !ok {
		project, loc, repo = "<project>", "<location>", "<repo>"
	}
	return fmt.Sprintf("gcloud artifacts repositories add-iam-policy-binding %s --project=%s --location=%s --member=serviceAccount:%s --role=roles/artifactregistry.reader", repo, project, loc, email)
}

func grantAteletPermissions(ctx context.Context, cfg *Config) error {
	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	resource := fmt.Sprintf("projects/%s", cfg.ProjectID)
	req := &iampb.GetIamPolicyRequest{
		Resource: resource,
	}

	policy, err := client.GetIamPolicy(ctx, req)
	if err != nil {
		return fmt.Errorf("get project iam policy: %w", err)
	}

	member := fmt.Sprintf("principal://iam.googleapis.com/projects/%s/locations/global/workloadIdentityPools/%s.svc.id.goog/subject/ns/ate-system/sa/atelet", cfg.ProjectNumber, cfg.ProjectID)

	// atelet gets no storage role here, see bucketGrants.
	// TODO: grant artifactregistry.reader on the repository in use only.
	changed := addProjectIamBinding(policy, "roles/artifactregistry.reader", member)

	if !changed {
		slog.Info("IAM policy already has required GKE node permissions. Skipping update.", slog.String("project", cfg.ProjectID))
		return nil
	}

	slog.Info("Setting IAM policy (grant api server permissions)...", slog.String("project", cfg.ProjectID))
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

// validateIamFlags checks every flag the command needs before any of the
// grants run. Each grant is a separate SetIamPolicy call, so a flag validated
// partway through the sequence reports a usage error only after the earlier
// grants have already been written to the project.
func validateIamFlags(ctx context.Context, cfg *Config, bucketBindings bool) error {
	if err := resolveProject(ctx, cfg); err != nil {
		return err
	}
	if bucketBindings && cfg.BucketName == "" {
		return errors.New("--bucket is required for bucket bindings")
	}
	if bucketBindings {
		return checkAssetBucketName(cfg)
	}
	return nil
}

var iamCmd = &cobra.Command{
	Use:   "iam",
	Short: "Create IAM bindings",
	RunE: func(cmd *cobra.Command, args []string) error {
		gkeNodes, _ := cmd.Flags().GetBool("gke-nodes")
		atelet, _ := cmd.Flags().GetBool("atelet")
		transferAccount, _ := cmd.Flags().GetBool("transfer-service-account")
		bucketBindings, _ := cmd.Flags().GetBool("bucket-bindings")

		if err := validateIamFlags(cmd.Context(), &cfg, bucketBindings); err != nil {
			return err
		}

		if gkeNodes {
			if err := grantGkeNodePermissions(cmd.Context(), &cfg); err != nil {
				return err
			}
		}
		if atelet {
			if err := grantAteletPermissions(cmd.Context(), &cfg); err != nil {
				return err
			}
		}
		// Before the bucket bindings, which grant the account: IAM refuses a
		// binding for an account that does not exist.
		if transferAccount {
			if err := setupTransferServiceAccount(cmd.Context(), &cfg, workloadIdentityPrincipal(&cfg, "ate-api-server")); err != nil {
				return err
			}
		}
		if bucketBindings {
			if err := createIamPolicyBindings(cmd.Context(), &cfg); err != nil {
				return err
			}
		}
		return nil
	},
}

func init() {
	createCmd.AddCommand(iamCmd)
	iamCmd.Flags().StringVar(&cfg.BucketName, "bucket", getEnv("BUCKET_NAME", ""), "GCS bucket name (required for bucket bindings) [env: BUCKET_NAME]")
	iamCmd.Flags().Bool("gke-nodes", true, "Grant GKE nodes permission to pull images")
	iamCmd.Flags().Bool("atelet", true, "Grant atelet project-level permissions")
	addNodeGSANameFlag(iamCmd)
	addTransferGSANameFlag(iamCmd)
	iamCmd.Flags().Bool("transfer-service-account", true, "Create the snapshot transfer service account and let ate-api-server create tokens for it")
	iamCmd.Flags().Bool("bucket-bindings", true, "Grant ate-api-server and the snapshot transfer service account access to the snapshot bucket, and atelet read access to the asset bucket")
}
