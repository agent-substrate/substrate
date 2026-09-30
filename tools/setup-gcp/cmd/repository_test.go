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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"

	artifactregistry "google.golang.org/api/artifactregistry/v1"
	"google.golang.org/api/option"
)

func TestValidateRepositoryFlags(t *testing.T) {
	for _, tc := range []struct {
		name    string
		project string
		region  string
		repo    string
		wantErr bool
	}{
		{"default", "test-project", "us-west1", "ate-images", false},
		{"single letter", "test-project", "us-west1", "a", false},
		{"max length", "test-project", "us-west1", strings.Repeat("a", 63), false},
		{"missing project", "", "us-west1", "ate-images", true},
		{"missing region", "test-project", "", "ate-images", true},
		{"missing name", "test-project", "us-west1", "", true},
		{"too long", "test-project", "us-west1", strings.Repeat("a", 64), true},
		{"uppercase", "test-project", "us-west1", "Images", true},
		{"starts with digit", "test-project", "us-west1", "1-images", true},
		{"trailing hyphen", "test-project", "us-west1", "images-", true},
		{"path", "test-project", "us-west1", "images/nested", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{ProjectID: tc.project, Region: tc.region, ArtifactRegistryRepository: tc.repo}
			if err := validateRepositoryFlags(t.Context(), &cfg); (err != nil) != tc.wantErr {
				t.Fatalf("validateRepositoryFlags() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestEnsureArtifactRepository(t *testing.T) {
	const parent = "projects/test-project/locations/europe-west1"
	const repoPath = "/v1/" + parent + "/repositories/custom-images"
	const operationName = parent + "/operations/create-repository"
	const operationPath = "/v1/" + operationName
	const ready = `{"name":"` + parent + `/repositories/custom-images","format":"DOCKER","mode":"STANDARD_REPOSITORY"}`
	const missing = `{"error":{"code":404,"message":"not found"}}`
	type response struct {
		method string
		path   string
		code   int
		body   string
	}
	get := response{"GET", repoPath, 200, ready}
	notFound := response{"GET", repoPath, 404, missing}
	create := response{"POST", "/v1/" + parent + "/repositories", 200, `{"name":"` + operationName + `"}`}
	pending := response{"GET", operationPath, 200, `{"name":"` + operationName + `"}`}
	done := response{"GET", operationPath, 200, `{"name":"` + operationName + `","done":true}`}

	for _, tc := range []struct {
		name      string
		responses []response
		wantErr   string
	}{
		{"existing repository", []response{get}, ""},
		{"create and wait", []response{notFound, create, pending, done, get}, ""},
		{"immediate completion", []response{notFound, {"POST", create.path, 200, `{"done":true}`}, get}, ""},
		{"wrong format", []response{{"GET", repoPath, 200, `{"format":"MAVEN"}`}}, "want DOCKER"},
		{"remote repository", []response{{"GET", repoPath, 200, `{"format":"DOCKER","mode":"REMOTE_REPOSITORY"}`}}, "want STANDARD_REPOSITORY"},
		{"lookup denied", []response{{"GET", repoPath, 403, `{"error":{"code":403,"message":"lookup denied"}}`}}, "lookup denied"},
		{"create denied", []response{notFound, {"POST", create.path, 403, `{"error":{"code":403,"message":"create denied"}}`}}, "create denied"},
		{"concurrent create", []response{notFound, {"POST", create.path, 409, `{"error":{"code":409,"message":"already exists"}}`}, get}, ""},
		{"operation failed", []response{notFound, create, {"GET", operationPath, 200, `{"done":true,"error":{"code":7,"message":"operation denied"}}`}}, "operation denied"},
		{"poll failed", []response{notFound, create, {"GET", operationPath, 403, `{"error":{"code":403,"message":"poll denied"}}`}}, "poll denied"},
		{"malformed operation", []response{notFound, {"POST", create.path, 200, `{}`}}, "without a name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				index := len(calls)
				calls = append(calls, r.Method+" "+r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				if index >= len(tc.responses) {
					t.Errorf("unexpected request: %s", calls[index])
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				expected := tc.responses[index]
				if r.Method != expected.method || r.URL.Path != expected.path {
					t.Errorf("request = %s, want %s %s", calls[index], expected.method, expected.path)
				}
				if r.Method == "POST" {
					if got := r.URL.Query().Get("repositoryId"); got != "custom-images" {
						t.Errorf("repositoryId = %q, want custom-images", got)
					}
					var body artifactregistry.Repository
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Errorf("decode repository: %v", err)
					}
					if body.Format != "DOCKER" || body.Mode != "STANDARD_REPOSITORY" {
						t.Errorf("created format/mode = %s/%s, want DOCKER/STANDARD_REPOSITORY", body.Format, body.Mode)
					}
				}
				w.WriteHeader(expected.code)
				fmt.Fprint(w, expected.body)
			}))
			defer server.Close()
			svc, err := artifactregistry.NewService(t.Context(), option.WithEndpoint(server.URL+"/"), option.WithHTTPClient(server.Client()))
			if err != nil {
				t.Fatal(err)
			}
			cfg := Config{ProjectID: "test-project", Region: "europe-west1", ArtifactRegistryRepository: "custom-images"}
			err = ensureArtifactRepository(t.Context(), svc, &cfg)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("ensureArtifactRepository() = %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("ensureArtifactRepository() = %v, want %q", err, tc.wantErr)
			}
			if len(calls) != len(tc.responses) {
				t.Errorf("got %d requests, want %d: %v", len(calls), len(tc.responses), calls)
			}
		})
	}
}

func TestRepositoryOperationCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cancel()
		fmt.Fprint(w, `{"name":"projects/p/locations/l/operations/o"}`)
	}))
	defer server.Close()
	defer cancel()
	svc, err := artifactregistry.NewService(ctx, option.WithEndpoint(server.URL+"/"), option.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	err = waitForRepositoryOperation(ctx, svc, &artifactregistry.Operation{Name: "projects/p/locations/l/operations/o"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForRepositoryOperation() = %v, want context.Canceled", err)
	}
}

func TestMakefileRegistryConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  []string
		args []string
		want string
	}{
		{"defaults", []string{"PROJECT_ID=test-project"}, nil, "us-west1-docker.pkg.dev/test-project/ate-images"},
		{"region and repository", []string{"PROJECT_ID=test-project", "GCE_REGION=europe-west1", "ARTIFACT_REGISTRY_REPOSITORY=custom-images"}, nil, "europe-west1-docker.pkg.dev/test-project/custom-images"},
		{"kind environment", []string{"KO_DOCKER_REPO=localhost:5001"}, nil, "localhost:5001"},
		{"legacy environment", []string{"KO_DOCKER_REPO=gcr.io/test-project/ate-images"}, nil, "gcr.io/test-project/ate-images"},
		{"command line wins", []string{"KO_DOCKER_REPO=localhost:5001"}, []string{"KO_DOCKER_REPO=registry.example.com/images"}, "registry.example.com/images"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"--no-print-directory", "-s", "-f", "../../../Makefile", "-f", "-", "print-repository"}, tc.args...)
			cmd := exec.CommandContext(t.Context(), "make", args...)
			for _, entry := range os.Environ() {
				key, _, _ := strings.Cut(entry, "=")
				switch key {
				case "PROJECT_ID", "GCE_REGION", "ARTIFACT_REGISTRY_REPOSITORY", "KO_DOCKER_REPO", "MAKEFLAGS", "MFLAGS", "MAKEOVERRIDES", "MAKELEVEL":
					continue
				}
				cmd.Env = append(cmd.Env, entry)
			}
			cmd.Env = append(cmd.Env, tc.env...)
			cmd.Stdin = strings.NewReader("print-repository:\n\t@printenv KO_DOCKER_REPO\n")
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("make: %v\n%s", err, out)
			}
			if got := strings.TrimSpace(string(out)); got != tc.want {
				t.Errorf("exported KO_DOCKER_REPO = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRepositoryCommandDefaults(t *testing.T) {
	cmd, _, err := rootCmd.Find([]string{"create", "repository"})
	if err != nil {
		t.Fatal(err)
	}
	want := getEnv("ARTIFACT_REGISTRY_REPOSITORY", "ate-images")
	if got := cmd.Flags().Lookup("name").DefValue; got != want {
		t.Errorf("create repository --name default = %q, want %q", got, want)
	}
	if got := bootstrapCmd.Flags().Lookup("repository-name").DefValue; got != want {
		t.Errorf("bootstrap --repository-name default = %q, want %q", got, want)
	}
}
