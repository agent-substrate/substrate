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

# Image naming, kept out of KO_FLAGS so that overriding those does not drop it.
# Each image is published as <repo>/<last element of its import path>, which is
# the name `ate-setup deploy --image-repo` looks for. ko's default appends an
# md5 of the full import path instead, which nothing outside ko can predict.
# cmd/ate-setup/internal/ko passes the same flag.
KO_NAMING := --base-import-paths

# Image tags, kept out of KO_FLAGS for the same reason. Empty by default, so ko
# tags `latest`; build-release-images sets it to $(VERSION).
KO_TAGS :=

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
# none for an install from a release. envoy-dataplane is built from a
# Dockerfile, not with ko, so it is in neither; see build-envoy-dataplane.
CONTROL_PLANE_IMAGES := ./cmd/ateapi \
                        ./cmd/atecontroller \
                        ./cmd/atelet \
                        ./cmd/atenet \
                        ./cmd/credential-provider/kubernetes-secrets \
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

# Images ko cannot build: envoy-dataplane is Envoy plus a Rust dynamic module.
# It is pushed as $(KO_DOCKER_REPO)/envoy-dataplane:$(VERSION), beside the ko
# images, where `ate-setup deploy --image-repo --image-tag` looks for it. A
# build from source doesn't need this; ate-setup builds the image itself.
#
# Platforms default to KO_DEFAULTPLATFORMS, else linux/amd64, as in ate-setup.
# Another architecture compiles Rust under QEMU, which the build host must have
# registered with binfmt. Extra buildx flags go in DOCKER_BUILD_FLAGS.
DOCKERFILE_PLATFORMS ?= $(or $(KO_DEFAULTPLATFORMS),linux/amd64)
DOCKER_BUILD_FLAGS   ?=

.PHONY: all
all: build

.PHONY: build
build: build-images build-atectl build-ate-setup

.PHONY: build-images
build-images:
	$(KO) build $(KO_NAMING) $(KO_TAGS) $(KO_FLAGS) \
	    --ldflags="$(LDFLAGS)" \
	    $(IMAGES)

.PHONY: build-atectl
build-atectl:
	$(GO) build -ldflags "$(LDFLAGS)" -o $(ATECTL) ./cmd/kubectl-ate

# The cluster installer. hack/install-ate.sh is a shim over it; see
# cmd/ate-setup/commands.md for the flag-by-flag mapping between the two.
.PHONY: build-ate-setup
build-ate-setup:
	$(GO) build -ldflags "$(LDFLAGS)" -o $(ATESETUP) ./cmd/ate-setup

.PHONY: build-atenet
build-atenet:
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BINDIR)/atenet ./cmd/atenet

.PHONY: build-demos
build-demos:
	$(KO) build $(KO_NAMING) $(KO_TAGS) $(KO_FLAGS) \
	    --ldflags="$(LDFLAGS)" \
	    $(DEMOS)

.PHONY: build-envoy-dataplane
build-envoy-dataplane:
	docker buildx build --push $(DOCKER_BUILD_FLAGS) \
	    --platform=$(DOCKERFILE_PLATFORMS) \
	    -t $(KO_DOCKER_REPO)/envoy-dataplane:$(VERSION) \
	    cmd/dataplane/envoy

# Every image a pre-built install needs, all tagged $(VERSION), which is what
# `ate-setup deploy --image-repo $(KO_DOCKER_REPO) --image-tag $(VERSION)`
# installs. Stage a release with e.g.
#   make build-release-images KO_DOCKER_REPO=REPO VERSION=TAG
.PHONY: build-release-images
build-release-images: KO_TAGS = --tags=$(VERSION)
build-release-images: build-images build-demos build-envoy-dataplane

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
