# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ko pushes to KO_DOCKER_REPO, e.g. localhost:5001 for kind. --base-import-paths
# names the image gsm-credential-provider, without a hash suffix.
KO       ?= go run github.com/google/ko@v0.19.1
KO_FLAGS ?= --base-import-paths

# The toolchain's own gofmt, not whichever one is first on PATH.
GOFMT := $(shell go env GOROOT)/bin/gofmt

.PHONY: all
all: verify test

.PHONY: build
build:
	go build ./...

.PHONY: test
test:
	go test -race ./...

.PHONY: fmt
fmt:
	$(GOFMT) -w .

.PHONY: lint
lint:
	../../../hack/run-tool.sh golangci-lint run --config ../../../.golangci.yaml ./...

.PHONY: verify
verify:
	@out="$$($(GOFMT) -l .)"; if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi
	go vet ./...
	go mod tidy -diff

# Builds the image and applies the manifests to the current kubectl context.
.PHONY: deploy
deploy:
	kubectl kustomize config | $(KO) apply $(KO_FLAGS) -f -

# Renders the manifests with the image pinned by digest, for a release.
.PHONY: release-manifest
release-manifest:
	kubectl kustomize config | $(KO) resolve $(KO_FLAGS) -f -

.PHONY: undeploy
undeploy:
	kubectl delete -k config --ignore-not-found
