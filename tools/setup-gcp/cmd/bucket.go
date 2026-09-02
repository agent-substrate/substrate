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
	"slices"
	"strconv"
	"strings"

	"cloud.google.com/go/iam"
	"cloud.google.com/go/storage"
	"github.com/spf13/cobra"
)

func createSnapshotBucket(ctx context.Context, cfg *Config) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	bucket := client.Bucket(cfg.BucketName)
	slog.Info("Checking if Bucket exists", slog.String("bucket", cfg.BucketName))
	attrs, err := bucket.Attrs(ctx)
	if err != nil {
		if !errors.Is(err, storage.ErrBucketNotExist) {
			return fmt.Errorf("getting bucket: %w", err)
		}

		slog.Info("Bucket does not exist. Creating...", slog.String("bucket", cfg.BucketName))
		err = bucket.Create(ctx, cfg.ProjectID, &storage.BucketAttrs{
			Location: cfg.Region,
			UniformBucketLevelAccess: storage.UniformBucketLevelAccess{
				Enabled: true,
			},
		})
		if err != nil {
			return fmt.Errorf("create snapshot bucket: %w", err)
		}
		return nil
	}

	slog.Info("Bucket exists. Checking attributes...", slog.String("bucket", cfg.BucketName))

	// Ensure the bucket belongs to the correct project.
	// GCS bucket names are globally unique, so it's possible this bucket belongs to a different project.
	projectNum := strconv.FormatUint(attrs.ProjectNumber, 10)
	if projectNum != cfg.ProjectNumber {
		return fmt.Errorf("bucket %s belongs to project number %s, but expected %s (it may be owned by another GCP project)", cfg.BucketName, projectNum, cfg.ProjectNumber)
	}

	// Ensure the bucket is in the correct region.
	if !strings.EqualFold(attrs.Location, cfg.Region) {
		return fmt.Errorf("bucket %s is in location %s, but expected %s", cfg.BucketName, attrs.Location, cfg.Region)
	}

	slog.Info("Bucket is in the correct project and region. Checking uniform-bucket-level-access setting...", slog.String("bucket", cfg.BucketName))
	if !attrs.UniformBucketLevelAccess.Enabled {
		slog.Info("Updating uniform-bucket-level-access", slog.String("bucket", cfg.BucketName))
		_, err = bucket.Update(ctx, storage.BucketAttrsToUpdate{
			UniformBucketLevelAccess: &storage.UniformBucketLevelAccess{
				Enabled: true,
			},
		})
		if err != nil {
			return fmt.Errorf("update bucket ubla: %w", err)
		}
	} else {
		slog.Info("uniform-bucket-level-access is already correct", slog.String("bucket", cfg.BucketName))
	}

	return nil
}

// snapshotBucketRoles are the roles every Substrate component that touches the
// snapshot bucket needs: objectAdmin to read, write and delete the objects a
// snapshot is made of, and bucketViewer to resolve the bucket itself.
var snapshotBucketRoles = []string{"roles/storage.objectAdmin", "roles/storage.bucketViewer"}

// snapshotBucketSubjects are the Kubernetes service accounts granted those
// roles. atelet writes and reads external snapshots; ate-api-server copies
// them for tags and deletes the ones nothing refers to any more.
var snapshotBucketSubjects = []string{"atelet", "ate-api-server"}

func createIamPolicyBindings(ctx context.Context, cfg *Config) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	bucket := client.Bucket(cfg.BucketName)
	policy, err := bucket.IAM().Policy(ctx)
	if err != nil {
		return fmt.Errorf("get bucket iam policy: %w", err)
	}

	changed := false
	for _, subject := range snapshotBucketSubjects {
		member := fmt.Sprintf("principal://iam.googleapis.com/projects/%s/locations/global/workloadIdentityPools/%s.svc.id.goog/subject/ns/ate-system/sa/%s", cfg.ProjectNumber, cfg.ProjectID, subject)
		for _, role := range snapshotBucketRoles {
			if hasUnconditionalBinding(policy, role, member) {
				continue
			}
			slog.Info("Adding role to member", slog.String("bucket", cfg.BucketName), slog.String("role", role), slog.String("member", member))
			policy.Add(member, iam.RoleName(role))
			changed = true
		}
	}
	if !changed {
		slog.Info("IAM policy is already correct", slog.String("bucket", cfg.BucketName))
		return nil
	}

	slog.Info("Setting IAM policy for bucket", slog.String("bucket", cfg.BucketName))
	err = bucket.IAM().SetPolicy(ctx, policy)
	if err != nil {
		return fmt.Errorf("set bucket iam policy: %w", err)
	}

	return nil
}

// hasUnconditionalBinding reports whether the policy already grants role to
// member without a condition. Conditional bindings are ignored: they may not
// apply to the objects Substrate touches.
func hasUnconditionalBinding(policy *iam.Policy, role, member string) bool {
	for _, b := range policy.InternalProto.Bindings {
		if b.Condition == nil && b.Role == role && slices.Contains(b.Members, member) {
			return true
		}
	}
	return false
}

var bucketCmd = &cobra.Command{
	Use:   "bucket",
	Short: "Create GCS bucket",
	RunE: func(cmd *cobra.Command, args []string) error {
		if cfg.ProjectID == "" {
			return errors.New("--project-id is required")
		}
		if cfg.BucketName == "" {
			return errors.New("--name is required")
		}
		return createSnapshotBucket(cmd.Context(), &cfg)
	},
}

func init() {
	createCmd.AddCommand(bucketCmd)
	bucketCmd.Flags().StringVar(&cfg.BucketName, "name", getEnv("BUCKET_NAME", ""), "Name of the GCS bucket [env: BUCKET_NAME]")
}
