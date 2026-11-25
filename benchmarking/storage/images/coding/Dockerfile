# Copyright 2026 The Agent Substrate Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Actor image for the coding scenarios: git and a Linux kernel toolchain, with
# ate-env-guest serving the process API that wakestorm -phase script uses.
# ate-env-guest comes from the agent-substrate/env commit that
# benchmarking/workloads/manifests/ate-env-template.yaml.tmpl also uses.
#
#   docker build --platform linux/amd64 -t <registry>/storage-bench-coding \
#     benchmarking/storage/images/coding

FROM golang:1.27-bookworm@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea AS guest
RUN CGO_ENABLED=0 GOBIN=/out go install \
    github.com/agent-substrate/env/cmd/ate-env-guest@1c08a7a578afd896373511a83beb242aec1425c0

FROM debian:bookworm@sha256:268fb24c39bca92d326dd83ea3abebe4a3f7751c804ffef23bb6d857dd343ff8
RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates build-essential bc bison flex libelf-dev libssl-dev cpio kmod && \
    rm -rf /var/lib/apt/lists/*
RUN git config --system user.email bench@example.com && \
    git config --system user.name bench && \
    git config --system safe.directory '*'
COPY --from=guest /out/ate-env-guest /ate-env-guest
ENTRYPOINT ["/ate-env-guest", "--listen=:80"]
