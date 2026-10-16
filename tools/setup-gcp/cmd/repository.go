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
	"regexp"
	"time"

	"github.com/spf13/cobra"
	artifactregistry "google.golang.org/api/artifactregistry/v1"
	"google.golang.org/api/googleapi"
)

var repositoryNameRE = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$`)

func validateRepositoryFlags(ctx context.Context, cfg *Config) error {
	if err := resolveProjectID(ctx, cfg); err != nil {
		return err
	}
	if cfg.Region == "" {
		return errors.New("--region is required")
	}
	if !repositoryNameRE.MatchString(cfg.ArtifactRegistryRepository) {
		return errors.New("repository name must be 1-63 lowercase letters, digits or hyphens, starting with a letter and ending with a letter or digit")
	}
	return nil
}

func createArtifactRepository(ctx context.Context, cfg *Config) error {
	svc, err := artifactregistry.NewService(ctx)
	if err != nil {
		return fmt.Errorf("create artifact registry client: %w", err)
	}
	return ensureArtifactRepository(ctx, svc, cfg)
}

func ensureArtifactRepository(ctx context.Context, svc *artifactregistry.Service, cfg *Config) error {
	parent := fmt.Sprintf("projects/%s/locations/%s", cfg.ProjectID, cfg.Region)
	name := parent + "/repositories/" + cfg.ArtifactRegistryRepository
	repositories := svc.Projects.Locations.Repositories
	existing, err := repositories.Get(name).Context(ctx).Do()
	if err == nil {
		return checkArtifactRepository(existing)
	}
	if !isNotFound(err) {
		return fmt.Errorf("get artifact repository %s: %w", name, err)
	}

	slog.Info("Creating Artifact Registry repository", "repository", name)
	op, err := repositories.Create(parent, &artifactregistry.Repository{
		Format: "DOCKER",
		Mode:   "STANDARD_REPOSITORY",
	}).RepositoryId(cfg.ArtifactRegistryRepository).Context(ctx).Do()
	if err != nil {
		// Another bootstrap may have created it after our lookup.
		var apiErr *googleapi.Error
		if !errors.As(err, &apiErr) || apiErr.Code != 409 {
			return fmt.Errorf("create artifact repository %s: %w", name, err)
		}
	} else if err := waitForRepositoryOperation(ctx, svc, op); err != nil {
		return err
	}

	existing, err = repositories.Get(name).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get created artifact repository %s: %w", name, err)
	}
	return checkArtifactRepository(existing)
}

func checkArtifactRepository(repo *artifactregistry.Repository) error {
	if repo.Format != "DOCKER" {
		return fmt.Errorf("artifact repository %s has format %q, want DOCKER", repo.Name, repo.Format)
	}
	if repo.Mode != "" && repo.Mode != "STANDARD_REPOSITORY" {
		return fmt.Errorf("artifact repository %s has mode %q, want STANDARD_REPOSITORY for image pushes", repo.Name, repo.Mode)
	}
	slog.Info("Artifact Registry repository is ready", "repository", repo.Name)
	return nil
}

func waitForRepositoryOperation(ctx context.Context, svc *artifactregistry.Service, op *artifactregistry.Operation) error {
	for {
		if op.Done {
			if op.Error != nil {
				return fmt.Errorf("artifact repository operation %s failed (code %d): %s", op.Name, op.Error.Code, op.Error.Message)
			}
			return nil
		}
		if op.Name == "" {
			return errors.New("artifact registry returned an unfinished operation without a name")
		}
		name := op.Name
		var err error
		op, err = svc.Projects.Locations.Operations.Get(name).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("poll artifact repository operation %s: %w", name, err)
		}
		if !op.Done {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
	}
}

func init() {
	repositoryCmd := &cobra.Command{
		Use:   "repository",
		Short: "Create an Artifact Registry Docker repository",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateRepositoryFlags(cmd.Context(), &cfg); err != nil {
				return err
			}
			return createArtifactRepository(cmd.Context(), &cfg)
		},
	}
	repositoryCmd.Flags().StringVar(&cfg.ArtifactRegistryRepository, "name", getEnv("ARTIFACT_REGISTRY_REPOSITORY", "ate-images"), "Name of the Artifact Registry Docker repository [env: ARTIFACT_REGISTRY_REPOSITORY]")
	createCmd.AddCommand(repositoryCmd)
}
