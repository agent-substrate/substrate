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
	"strings"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	serviceusage "cloud.google.com/go/serviceusage/apiv1"
	"cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"github.com/spf13/cobra"
	"google.golang.org/api/googleapi"
	iam "google.golang.org/api/iam/v1"
	servicenetworking "google.golang.org/api/servicenetworking/v1"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

// The ateapi store database and the Kubernetes identity that connects to it.
// These match the atepg store (hack/install-ate.sh) and the ate-api-server
// deployment.
const (
	cloudSQLDatabase   = "atepg"
	apiServerNamespace = "ate-system"
	apiServerKSA       = "ate-api-server"
)

func cloudSQLGSAEmail(cfg *Config) string {
	return fmt.Sprintf("%s@%s.iam.gserviceaccount.com", cfg.CloudSQLGSAName, cfg.ProjectID)
}

// cloudSQLDatabaseUser derives the IAM database username for a service
// account: the email with the .gserviceaccount.com suffix trimmed.
func cloudSQLDatabaseUser(gsaEmail string) string {
	return strings.TrimSuffix(gsaEmail, ".gserviceaccount.com")
}

// workloadIdentityMember is the classic Workload Identity member for the
// ate-api-server KSA. WIF-direct principal:// identities (used elsewhere in
// this tool) cannot be Cloud SQL IAM database users, so Cloud SQL requires
// the KSA→GSA link.
func workloadIdentityMember(projectID string) string {
	return fmt.Sprintf("serviceAccount:%s.svc.id.goog[%s/%s]", projectID, apiServerNamespace, apiServerKSA)
}

func privateNetworkURL(cfg *Config) string {
	return fmt.Sprintf("projects/%s/global/networks/%s", cfg.ProjectID, cfg.Network)
}

func isNotFound(err error) bool {
	var gerr *googleapi.Error
	return errors.As(err, &gerr) && gerr.Code == 404
}

// enableCloudSQLAPIs idempotently enables the APIs this command depends on.
// Scoped here rather than in `enable apis`: Cloud SQL is opt-in, so only its
// users get these APIs turned on.
func enableCloudSQLAPIs(ctx context.Context, cfg *Config) error {
	suClient, err := serviceusage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer suClient.Close()

	services := []string{
		"sqladmin.googleapis.com",
		"servicenetworking.googleapis.com",
	}
	slog.Info("Batch enabling services", slog.String("services", strings.Join(services, ", ")))
	op, err := suClient.BatchEnableServices(ctx, &serviceusagepb.BatchEnableServicesRequest{
		Parent:     fmt.Sprintf("projects/%s", cfg.ProjectID),
		ServiceIds: services,
	})
	if err != nil {
		return fmt.Errorf("failed to start batch enabling services: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("failed to complete batch enabling services: %w", err)
	}
	return nil
}

// checkPrivateServicesAccess verifies the VPC has a private services access
// peering, which Cloud SQL private IP requires. Allocating the range and
// peering is a rare one-time-per-VPC operation left to gcloud.
func checkPrivateServicesAccess(ctx context.Context, cfg *Config) error {
	svc, err := servicenetworking.NewService(ctx)
	if err != nil {
		return fmt.Errorf("create servicenetworking client: %w", err)
	}
	resp, err := svc.Services.Connections.List("services/servicenetworking.googleapis.com").
		Network(privateNetworkURL(cfg)).Context(ctx).Do()
	if err == nil && len(resp.Connections) > 0 && len(resp.Connections[0].ReservedPeeringRanges) > 0 {
		return nil
	}
	if err != nil {
		slog.Warn("Could not list service networking connections", slog.Any("err", err))
	}
	return fmt.Errorf(`network %q has no private services access peering, which Cloud SQL private IP requires. Create it once per VPC:

  gcloud compute addresses create google-managed-services-%[1]s \
    --global --purpose=VPC_PEERING --prefix-length=16 --network=%[1]s --project=%[2]s
  gcloud services vpc-peerings connect \
    --service=servicenetworking.googleapis.com \
    --ranges=google-managed-services-%[1]s --network=%[1]s --project=%[2]s`,
		cfg.Network, cfg.ProjectID)
}

func waitForSQLOperation(ctx context.Context, svc *sqladmin.Service, cfg *Config, op *sqladmin.Operation) error {
	for {
		if op.Status == "DONE" {
			if op.Error != nil && len(op.Error.Errors) > 0 {
				return fmt.Errorf("operation %s failed: %s", op.Name, op.Error.Errors[0].Message)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
		name := op.Name
		var err error
		op, err = svc.Operations.Get(cfg.ProjectID, name).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("poll operation %s: %w", name, err)
		}
	}
}

// createCloudSQLInstance creates a private-IP PostgreSQL instance with IAM
// database authentication enabled, or verifies an existing one.
func createCloudSQLInstance(ctx context.Context, svc *sqladmin.Service, cfg *Config) error {
	slog.Info("Checking if Cloud SQL instance exists", slog.String("instance", cfg.CloudSQLInstance))
	existing, err := svc.Instances.Get(cfg.ProjectID, cfg.CloudSQLInstance).Context(ctx).Do()
	if err == nil {
		iamAuthOn := false
		if existing.Settings != nil {
			for _, f := range existing.Settings.DatabaseFlags {
				if f.Name == "cloudsql.iam_authentication" && f.Value == "on" {
					iamAuthOn = true
				}
			}
		}
		if !iamAuthOn {
			return fmt.Errorf("instance %s exists but cloudsql.iam_authentication is off; enable it (this restarts the instance):\n\n  gcloud sql instances patch %s --database-flags=cloudsql.iam_authentication=on --project=%s",
				cfg.CloudSQLInstance, cfg.CloudSQLInstance, cfg.ProjectID)
		}
		slog.Info("Cloud SQL instance exists with IAM authentication on. Skipping create.")
		return nil
	}
	if !isNotFound(err) {
		return fmt.Errorf("get instance: %w", err)
	}

	if err := checkPrivateServicesAccess(ctx, cfg); err != nil {
		return err
	}

	spec, err := cloudSQLInstanceSpec(cfg)
	if err != nil {
		return err
	}
	slog.Info("Creating Cloud SQL instance (takes several minutes)...",
		slog.String("instance", cfg.CloudSQLInstance), slog.String("region", cfg.Region),
		slog.String("tier", cfg.CloudSQLTier), slog.String("edition", spec.Settings.Edition))
	op, err := svc.Instances.Insert(cfg.ProjectID, spec).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create instance: %w", err)
	}
	return waitForSQLOperation(ctx, svc, cfg, op)
}

// cloudSQLInstanceSpec maps the flags onto the creation request. Pure, for
// testing. Shape/storage of an already-existing instance is never touched —
// resize with `gcloud sql instances patch`.
func cloudSQLInstanceSpec(cfg *Config) (*sqladmin.DatabaseInstance, error) {
	settings := &sqladmin.Settings{
		Tier: cfg.CloudSQLTier,
		// This tool provisions private-services-access instances exclusively.
		// The ATE_API_POSTGRES_CLOUDSQL_IP_TYPE setting in install-ate.sh exists
		// only to connect to public/PSC instances provisioned out-of-band.
		IpConfiguration: &sqladmin.IpConfiguration{
			Ipv4Enabled:     false,
			PrivateNetwork:  privateNetworkURL(cfg),
			ForceSendFields: []string{"Ipv4Enabled"},
		},
		DatabaseFlags: []*sqladmin.DatabaseFlags{
			{Name: "cloudsql.iam_authentication", Value: "on"},
		},
		// Daily automated backups with point-in-time recovery, and a guard
		// against accidental deletion; clear it with `gcloud sql instances
		// patch --no-deletion-protection` before tearing the instance down.
		BackupConfiguration: &sqladmin.BackupConfiguration{
			Enabled:                    true,
			PointInTimeRecoveryEnabled: true,
		},
		DeletionProtectionEnabled: true,
	}
	switch cfg.CloudSQLEdition {
	case "", "enterprise":
		// Enterprise edition accepts db-custom-<vCPU>-<MB> tiers.
		settings.Edition = "ENTERPRISE"
	case "enterprise-plus":
		// Enterprise Plus only accepts db-perf-optimized-N-<vCPU> tiers. Its
		// local-SSD data cache is the reason to pick it for this store: it
		// extends the effective cache beyond RAM once the dataset outgrows
		// memory (see cloud-sql.md, "Scaling the database").
		settings.Edition = "ENTERPRISE_PLUS"
		settings.DataCacheConfig = &sqladmin.DataCacheConfig{DataCacheEnabled: true}
	default:
		return nil, fmt.Errorf("unknown --edition %q (want enterprise|enterprise-plus)", cfg.CloudSQLEdition)
	}
	// 0 leaves the Cloud SQL default (10 GB, auto-resizing). PD IOPS and
	// throughput scale with provisioned size, so benchmarks and production
	// should pre-size rather than rely on auto-resize.
	if cfg.CloudSQLStorageGB > 0 {
		settings.DataDiskSizeGb = cfg.CloudSQLStorageGB
	}
	return &sqladmin.DatabaseInstance{
		Name:            cfg.CloudSQLInstance,
		Region:          cfg.Region,
		DatabaseVersion: "POSTGRES_18",
		Settings:        settings,
	}, nil
}

func createCloudSQLDatabase(ctx context.Context, svc *sqladmin.Service, cfg *Config) error {
	_, err := svc.Databases.Get(cfg.ProjectID, cfg.CloudSQLInstance, cloudSQLDatabase).Context(ctx).Do()
	if err == nil {
		slog.Info("Database exists. Skipping create.", slog.String("database", cloudSQLDatabase))
		return nil
	}
	if !isNotFound(err) {
		return fmt.Errorf("get database: %w", err)
	}
	slog.Info("Creating database...", slog.String("database", cloudSQLDatabase))
	op, err := svc.Databases.Insert(cfg.ProjectID, cfg.CloudSQLInstance, &sqladmin.Database{
		Name: cloudSQLDatabase,
	}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	return waitForSQLOperation(ctx, svc, cfg, op)
}

func createCloudSQLGSA(ctx context.Context, cfg *Config) error {
	svc, err := iam.NewService(ctx)
	if err != nil {
		return fmt.Errorf("create iam client: %w", err)
	}
	email := cloudSQLGSAEmail(cfg)
	resource := fmt.Sprintf("projects/%s/serviceAccounts/%s", cfg.ProjectID, email)
	_, err = svc.Projects.ServiceAccounts.Get(resource).Context(ctx).Do()
	if err == nil {
		slog.Info("Service account exists. Skipping create.", slog.String("gsa", email))
	} else if isNotFound(err) {
		slog.Info("Creating service account...", slog.String("gsa", email))
		_, err = svc.Projects.ServiceAccounts.Create("projects/"+cfg.ProjectID, &iam.CreateServiceAccountRequest{
			AccountId: cfg.CloudSQLGSAName,
			ServiceAccount: &iam.ServiceAccount{
				DisplayName: "ate-api-server Cloud SQL access",
			},
		}).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("create service account: %w", err)
		}
	} else {
		return fmt.Errorf("get service account: %w", err)
	}

	// Classic Workload Identity: let the ate-api-server KSA mint tokens as
	// this GSA, so the proxy sidecar's ADC resolves to the IAM database user.
	member := workloadIdentityMember(cfg.ProjectID)
	policy, err := svc.Projects.ServiceAccounts.GetIamPolicy(resource).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get service account iam policy: %w", err)
	}
	const wiRole = "roles/iam.workloadIdentityUser"
	for _, b := range policy.Bindings {
		if b.Role == wiRole {
			for _, m := range b.Members {
				if m == member {
					slog.Info("Workload Identity binding exists. Skipping.")
					return nil
				}
			}
			b.Members = append(b.Members, member)
			return setGSAPolicy(ctx, svc, resource, policy)
		}
	}
	policy.Bindings = append(policy.Bindings, &iam.Binding{Role: wiRole, Members: []string{member}})
	return setGSAPolicy(ctx, svc, resource, policy)
}

func setGSAPolicy(ctx context.Context, svc *iam.Service, resource string, policy *iam.Policy) error {
	slog.Info("Binding Workload Identity user to service account...")
	_, err := svc.Projects.ServiceAccounts.SetIamPolicy(resource, &iam.SetIamPolicyRequest{Policy: policy}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("set service account iam policy: %w", err)
	}
	return nil
}

// grantCloudSQLProjectRoles grants the GSA the roles needed to establish
// connector tunnels (cloudsql.client) and to log in with IAM database
// authentication (cloudsql.instanceUser).
func grantCloudSQLProjectRoles(ctx context.Context, cfg *Config) error {
	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	resource := fmt.Sprintf("projects/%s", cfg.ProjectID)
	policy, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: resource})
	if err != nil {
		return fmt.Errorf("get project iam policy: %w", err)
	}

	member := "serviceAccount:" + cloudSQLGSAEmail(cfg)
	// A freshly created service account can take a while to propagate;
	// granting to it too soon fails with "does not exist". Re-read the
	// policy each attempt so etag conflicts also resolve.
	for attempt := 1; ; attempt++ {
		changed1 := addProjectIamBinding(policy, "roles/cloudsql.client", member)
		changed2 := addProjectIamBinding(policy, "roles/cloudsql.instanceUser", member)
		if !changed1 && !changed2 {
			slog.Info("IAM policy already has required Cloud SQL permissions. Skipping update.")
			return nil
		}
		slog.Info("Setting IAM policy (grant Cloud SQL permissions)...", slog.String("member", member))
		_, err = client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: resource, Policy: policy})
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
		policy, err = client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: resource})
		if err != nil {
			return fmt.Errorf("get project iam policy: %w", err)
		}
	}
}

func createCloudSQLIAMUser(ctx context.Context, svc *sqladmin.Service, cfg *Config) error {
	dbUser := cloudSQLDatabaseUser(cloudSQLGSAEmail(cfg))
	users, err := svc.Users.List(cfg.ProjectID, cfg.CloudSQLInstance).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("list database users: %w", err)
	}
	for _, u := range users.Items {
		if u.Name == dbUser {
			slog.Info("IAM database user exists. Skipping create.", slog.String("user", dbUser))
			return nil
		}
	}
	slog.Info("Creating IAM database user...", slog.String("user", dbUser))
	// The API requires the truncated form (without .gserviceaccount.com),
	// which is also the username the database sees.
	op, err := svc.Users.Insert(cfg.ProjectID, cfg.CloudSQLInstance, &sqladmin.User{
		Name: dbUser,
		Type: "CLOUD_IAM_SERVICE_ACCOUNT",
	}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create IAM database user: %w", err)
	}
	return waitForSQLOperation(ctx, svc, cfg, op)
}

func printCloudSQLNextSteps(cfg *Config) {
	gsa := cloudSQLGSAEmail(cfg)
	dbUser := cloudSQLDatabaseUser(gsa)
	fmt.Printf(`
Cloud SQL is provisioned. Two steps remain:

1. One-time schema privileges (PostgreSQL 15+ removed PUBLIC's CREATE on the
   public schema; ateapi applies its schema at startup as the IAM user).
   Connect as the postgres user and run:

     GRANT USAGE, CREATE ON SCHEMA public TO "%s";

2. Deploy ateapi against it:

     export ATE_API_POSTGRES_CLOUDSQL_INSTANCE=%s:%s:%s
     export ATE_API_POSTGRES_CLOUDSQL_GSA=%s
     ./hack/install-ate.sh --deploy-ate-system

See tools/setup-gcp/cloud-sql.md for details and verification steps.
`, dbUser, cfg.ProjectID, cfg.Region, cfg.CloudSQLInstance, gsa)
}

var cloudsqlCmd = &cobra.Command{
	Use:   "cloudsql",
	Short: "Create a Cloud SQL PostgreSQL instance for the ateapi store, with IAM database authentication",
	RunE: func(cmd *cobra.Command, args []string) error {
		if cfg.ProjectID == "" {
			return errors.New("--project-id is required")
		}
		// Require explicit region to prevent silent cross-region latency and egress costs.
		if !cmd.Flags().Changed("region") && os.Getenv("GCE_REGION") == "" {
			return errors.New("--region is required (or set GCE_REGION): the instance must be in the cluster's region")
		}
		ctx := cmd.Context()
		if err := enableCloudSQLAPIs(ctx, &cfg); err != nil {
			return err
		}
		svc, err := sqladmin.NewService(ctx)
		if err != nil {
			return fmt.Errorf("create sqladmin client: %w", err)
		}
		if err := createCloudSQLInstance(ctx, svc, &cfg); err != nil {
			return err
		}
		if err := createCloudSQLDatabase(ctx, svc, &cfg); err != nil {
			return err
		}
		if err := createCloudSQLGSA(ctx, &cfg); err != nil {
			return err
		}
		if err := grantCloudSQLProjectRoles(ctx, &cfg); err != nil {
			return err
		}
		if err := createCloudSQLIAMUser(ctx, svc, &cfg); err != nil {
			return err
		}
		printCloudSQLNextSteps(&cfg)
		return nil
	},
}

func init() {
	createCmd.AddCommand(cloudsqlCmd)
	cloudsqlCmd.Flags().StringVar(&cfg.CloudSQLInstance, "instance", getEnv("CLOUDSQL_INSTANCE", "atepg"), "Cloud SQL instance name [env: CLOUDSQL_INSTANCE]")
	cloudsqlCmd.Flags().StringVar(&cfg.CloudSQLTier, "tier", getEnv("CLOUDSQL_TIER", "db-custom-2-8192"), "Machine tier: db-custom-<vCPU>-<MB> for enterprise, db-perf-optimized-N-<vCPU> for enterprise-plus [env: CLOUDSQL_TIER]")
	cloudsqlCmd.Flags().StringVar(&cfg.CloudSQLEdition, "edition", getEnv("CLOUDSQL_EDITION", "enterprise"), "Instance edition: enterprise | enterprise-plus (enables the local-SSD data cache) [env: CLOUDSQL_EDITION]")
	cloudsqlCmd.Flags().Int64Var(&cfg.CloudSQLStorageGB, "storage-size", getEnv("CLOUDSQL_STORAGE_GB", int64(0)), "Data disk size in GB; 0 = Cloud SQL default (10 GB, auto-resizing). PD IOPS scale with size [env: CLOUDSQL_STORAGE_GB]")
	cloudsqlCmd.Flags().StringVar(&cfg.CloudSQLGSAName, "gsa-name", getEnv("CLOUDSQL_GSA_NAME", "ate-api-server"), "Name of the Google service account to create for Workload Identity + IAM database auth [env: CLOUDSQL_GSA_NAME]")
	cloudsqlCmd.Flags().StringVar(&cfg.Network, "network", getEnv("NETWORK", "default"), "VPC network name (must match the cluster's) [env: NETWORK]")
}
