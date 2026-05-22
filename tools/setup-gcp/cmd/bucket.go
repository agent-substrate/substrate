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
)

func createSnapshotBucket(ctx context.Context, env *Environment) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	bucket := client.Bucket(env.BucketName)
	slog.Info("Checking if Bucket exists", slog.String("bucket", env.BucketName))
	attrs, err := bucket.Attrs(ctx)
	if err != nil {
		if !errors.Is(err, storage.ErrBucketNotExist) {
			return fmt.Errorf("getting bucket: %w", err)
		}

		slog.Info("Bucket does not exist. Creating...", slog.String("bucket", env.BucketName))
		err = bucket.Create(ctx, env.ProjectID, &storage.BucketAttrs{
			Location: env.GCERegion,
			UniformBucketLevelAccess: storage.UniformBucketLevelAccess{
				Enabled: true,
			},
		})
		if err != nil {
			return fmt.Errorf("create snapshot bucket: %w", err)
		}
		return nil
	}

	slog.Info("Bucket exists. Checking attributes...", slog.String("bucket", env.BucketName))

	// Ensure the bucket belongs to the correct project.
	// GCS bucket names are globally unique, so it's possible this bucket belongs to a different project.
	projectNum := strconv.FormatUint(attrs.ProjectNumber, 10)
	if projectNum != env.ProjectNumber {
		return fmt.Errorf("bucket %s belongs to project number %s, but expected %s (it may be owned by another GCP project)", env.BucketName, projectNum, env.ProjectNumber)
	}

	// Ensure the bucket is in the correct region.
	if !strings.EqualFold(attrs.Location, env.GCERegion) {
		return fmt.Errorf("bucket %s is in location %s, but expected %s", env.BucketName, attrs.Location, env.GCERegion)
	}

	slog.Info("Bucket is in the correct project and region. Checking uniform-bucket-level-access setting...", slog.String("bucket", env.BucketName))
	if !attrs.UniformBucketLevelAccess.Enabled {
		slog.Info("Updating uniform-bucket-level-access", slog.String("bucket", env.BucketName))
		_, err = bucket.Update(ctx, storage.BucketAttrsToUpdate{
			UniformBucketLevelAccess: &storage.UniformBucketLevelAccess{
				Enabled: true,
			},
		})
		if err != nil {
			return fmt.Errorf("update bucket ubla: %w", err)
		}
	} else {
		slog.Info("uniform-bucket-level-access is already correct", slog.String("bucket", env.BucketName))
	}

	return nil
}

func createIamPolicyBindings(ctx context.Context, env *Environment) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	bucket := client.Bucket(env.BucketName)
	policy, err := bucket.IAM().Policy(ctx)
	if err != nil {
		return fmt.Errorf("get bucket iam policy: %w", err)
	}

	member := fmt.Sprintf("principal://iam.googleapis.com/projects/%s/locations/global/workloadIdentityPools/%s.svc.id.goog/subject/ns/ate-system/sa/atelet", env.ProjectNumber, env.ProjectID)

	hasStorageAdmin := false
	hasBucketViewer := false
	for _, b := range policy.InternalProto.Bindings {
		if b.Condition != nil {
			continue
		}
		if b.Role == "roles/storage.objectAdmin" && slices.Contains(b.Members, member) {
			hasStorageAdmin = true
		}
		if b.Role == "roles/storage.bucketViewer" && slices.Contains(b.Members, member) {
			hasBucketViewer = true
		}
		if hasStorageAdmin && hasBucketViewer {
			slog.Info("IAM policy is already correct", slog.String("bucket", env.BucketName), slog.String("member", member))
			return nil
		}
	}

	if !hasStorageAdmin {
		slog.Info("Adding storage.objectAdmin role to member", slog.String("bucket", env.BucketName), slog.String("member", member))
		policy.Add(member, iam.RoleName("roles/storage.objectAdmin"))
	}
	if !hasBucketViewer {
		slog.Info("Adding storage.bucketViewer role to member", slog.String("bucket", env.BucketName), slog.String("member", member))
		policy.Add(member, iam.RoleName("roles/storage.bucketViewer"))
	}

	slog.Info("Setting IAM policy for bucket", slog.String("bucket", env.BucketName))
	err = bucket.IAM().SetPolicy(ctx, policy)
	if err != nil {
		return fmt.Errorf("set bucket iam policy: %w", err)
	}

	return nil
}
