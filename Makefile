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

# Default project ID, can be overridden
PROJECT_ID ?= $(shell echo $${USER}-gke-dev)

# Ko configuration
export KO_DOCKER_REPO := gcr.io/$(PROJECT_ID)/ate-images

# Go commands
GO := go
KO := hack/run-tool.sh ko

# Binaries
BINDIR := bin/
ATECTL := $(BINDIR)/kubectl-ate

.PHONY: all
all: build

.PHONY: build
build: build-images build-atectl

.PHONY: build-images
build-images:
	$(KO) build ./cmd/ateapi
	$(KO) build ./cmd/atelet
	$(KO) build ./cmd/podcertcontroller
	$(KO) build ./cmd/atenet

.PHONY: build-atectl
build-atectl:
	$(GO) build -o $(ATECTL) ./cmd/kubectl-ate

.PHONY: build-atenet
build-atenet:
	$(GO) build -o $(BINDIR)/atenet ./cmd/atenet

.PHONY: build-demos
build-demos:
	$(KO) build ./demos/counter

.PHONY: test
test:
	$(GO) test ./...

.PHONY: e2e
e2e: build build-demos
	hack/run-e2e.sh

.PHONY: fmt verify-fmt

# Formats all Go files in the project
fmt:
	@./hack/update/gofmt.sh

# Fails if any Go files are not formatted properly
verify-fmt:
	@./hack/verify/gofmt.sh

.PHONY: lint

# Runs golangci-lint and fails on any reported issues.
lint:
	@./hack/verify/golangci-lint.sh

.PHONY: verify
verify: test
	$(GO) vet ./...
	bash hack/verify-all.sh

.PHONY: clean
clean:
	rm -rf $(BINDIR)
