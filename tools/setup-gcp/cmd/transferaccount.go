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
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/spf13/cobra"
	iam "google.golang.org/api/iam/v1"

	"go.ate.dev/substrate/internal/installdefaults"
)

// tokenCreatorRole lets a member create tokens for a service account, which
// ate-api-server's credential exchange does for the transfer account.
const tokenCreatorRole = "roles/iam.serviceAccountTokenCreator"

// addTransferGSANameFlag registers the transfer account's name on cmd, read
// from the variable ate-setup also reads.
func addTransferGSANameFlag(cmd *cobra.Command) {
	cmd.Flags().StringVar(&cfg.TransferGSAName, "transfer-gsa-name", getEnv("SNAPSHOT_TRANSFER_GSA_NAME", installdefaults.TransferServiceAccountName),
		"Name of the snapshot transfer service account to create [env: SNAPSHOT_TRANSFER_GSA_NAME]")
}

func transferGSAEmail(cfg *Config) string {
	return fmt.Sprintf("%s@%s.iam.gserviceaccount.com", cfg.TransferGSAName, cfg.ProjectID)
}

// transferServiceAccountMember is the transfer account as an IAM member.
func transferServiceAccountMember(cfg *Config) string {
	return "serviceAccount:" + transferGSAEmail(cfg)
}

// setupTransferServiceAccount creates the snapshot transfer service account
// unless it exists, and lets each of members create tokens for it.
func setupTransferServiceAccount(ctx context.Context, cfg *Config, members ...string) error {
	svc, err := iam.NewService(ctx)
	if err != nil {
		return fmt.Errorf("create iam client: %w", err)
	}
	email := transferGSAEmail(cfg)
	resource := fmt.Sprintf("projects/%s/serviceAccounts/%s", cfg.ProjectID, email)
	_, err = svc.Projects.ServiceAccounts.Get(resource).Context(ctx).Do()
	switch {
	case err == nil:
		slog.Info("Service account exists. Skipping create.", slog.String("gsa", email))
	case isNotFound(err):
		slog.Info("Creating service account...", slog.String("gsa", email))
		_, err = svc.Projects.ServiceAccounts.Create("projects/"+cfg.ProjectID, &iam.CreateServiceAccountRequest{
			AccountId:      cfg.TransferGSAName,
			ServiceAccount: &iam.ServiceAccount{DisplayName: "Substrate snapshot transfers"},
		}).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("create service account %s: %w", email, err)
		}
	default:
		return fmt.Errorf("get service account %s: %w", email, err)
	}

	// A service account created moments earlier, this one or a member, can
	// take a while to propagate, and granting too soon fails with "does not
	// exist". Re-read the policy each attempt so etag conflicts also resolve,
	// as grantCloudSQLProjectRoles does.
	for attempt := 1; ; attempt++ {
		err := grantTokenCreator(ctx, svc, resource, email, members)
		if err == nil {
			return nil
		}
		if attempt >= 6 {
			return err
		}
		slog.Warn("Granting token creator failed, retrying...", slog.String("gsa", email), slog.Int("attempt", attempt), slog.Any("err", err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
		}
	}
}

// grantTokenCreator grants members token creator on the service account.
func grantTokenCreator(ctx context.Context, svc *iam.Service, resource, email string, members []string) error {
	policy, err := svc.Projects.ServiceAccounts.GetIamPolicy(resource).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get service account %s iam policy: %w", email, err)
	}
	changed := false
	for _, member := range members {
		if addServiceAccountBinding(policy, tokenCreatorRole, member) {
			slog.Info("Granting token creator on service account", slog.String("gsa", email), slog.String("member", member))
			changed = true
		}
	}
	if !changed {
		slog.Info("Service account IAM policy is already correct", slog.String("gsa", email))
		return nil
	}
	return setGSAPolicy(ctx, svc, resource, policy)
}

// addServiceAccountBinding adds member to role in policy, and reports whether
// it added anything.
func addServiceAccountBinding(policy *iam.Policy, role, member string) bool {
	for _, b := range policy.Bindings {
		if b.Role != role || b.Condition != nil {
			continue
		}
		if slices.Contains(b.Members, member) {
			return false
		}
		b.Members = append(b.Members, member)
		return true
	}
	policy.Bindings = append(policy.Bindings, &iam.Binding{Role: role, Members: []string{member}})
	return true
}
