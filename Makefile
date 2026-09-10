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

# Flags every ko image build gets, e.g. `make build-images KO_FLAGS=--push=false`.
# Empty by default, so ko runs on its own defaults and whatever .ko.yaml configures.
KO_FLAGS ?=

# Binaries
BINDIR := bin/
ATECTL := $(BINDIR)/kubectl-ate
ATESETUP := $(BINDIR)/ate-setup

# Version stamping. Override on the make command line to pin
# (e.g. `make VERSION=v0.5.0 build`).
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
VERSION_PKG := github.com/agent-substrate/substrate/internal/version
LDFLAGS := -X=$(VERSION_PKG).Version=$(VERSION)

# Every image the installer can deploy, defined once. These two sets together
# have to cover images.Components in cmd/ate-setup/internal/images: a package
# missing here has no image for a build from source, and one missing there has
# none for an install from a release.
CONTROL_PLANE_IMAGES := ./cmd/ateapi \
                        ./cmd/atecontroller \
                        ./cmd/atelet \
                        ./cmd/atenet \
                        ./cmd/podcertcontroller
WORKER_IMAGES        := ./cmd/ateom-gvisor \
                        ./cmd/ateom-microvm
DEMO_IMAGES          := ./demos/counter \
                        ./demos/egress \
                        ./demos/multi-template/fspersist \
                        ./demos/sandbox
ALL_IMAGES           := $(CONTROL_PLANE_IMAGES) $(WORKER_IMAGES)

# Developer builds may leave components out, e.g. the microvm image, the one
# image built from a debian base rather than distroless static:
#   make build-images SKIP_IMAGES=./cmd/ateom-microvm
# Overriding IMAGES or DEMOS on the command line builds exactly that set.
SKIP_IMAGES ?=
IMAGES      := $(filter-out $(SKIP_IMAGES),$(ALL_IMAGES))
DEMOS       := $(filter-out $(SKIP_IMAGES),$(DEMO_IMAGES))

.PHONY: all
all: build

.PHONY: build
build: build-images build-atectl build-ate-setup

.PHONY: build-images
build-images:
	$(KO) build $(KO_FLAGS) \
	    --ldflags="$(LDFLAGS)" \
	    $(IMAGES)

.PHONY: build-atectl
build-atectl:
	$(GO) build -ldflags "$(LDFLAGS)" -o $(ATECTL) ./cmd/kubectl-ate

# The cluster installer, a Go port of hack/install-ate.sh. Both work today; see
# cmd/ate-setup/commands.md for the flag-by-flag mapping between them.
.PHONY: build-ate-setup
build-ate-setup:
	$(GO) build -ldflags "$(LDFLAGS)" -o $(ATESETUP) ./cmd/ate-setup

.PHONY: build-atenet
build-atenet:
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BINDIR)/atenet ./cmd/atenet

.PHONY: build-demos
build-demos:
	$(KO) build $(KO_FLAGS) \
	    --ldflags="$(LDFLAGS)" \
	    $(DEMOS)

.PHONY: test
test:
	$(GO) test -race ./...

.PHONY: e2e
e2e: build build-demos
	hack/run-e2e.sh

.PHONY: fmt verify-fmt

# Prints the Go ldflags (used for scripts to do version stamping).
ldflags:
	@for flag in $(LDFLAGS); do \
		echo $$flag; \
	done

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
	bash hack/verify-all.sh

.PHONY: clean
clean:
	rm -rf $(BINDIR)
