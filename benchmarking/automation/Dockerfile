# Copyright 2026 Google LLC
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

# Build context = benchmarking/automation/.
# Build with: docker build -t <image> benchmarking/automation/

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gettext-base \
        git \
        gnupg \
        make \
        python3 \
        python3-pip \
        python3-venv \
        docker.io \
    && rm -rf /var/lib/apt/lists/*

# Go must be new enough to honor the `tool` directive in
# hack/tools/*/go.mod (Go 1.24+). Pin a version known to match the
# substrate project's pinned ko (see hack/tools/ko/go.mod).
ARG GO_VERSION=1.26.3
RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" \
        | tar -C /usr/local -xz
ENV PATH=/usr/local/go/bin:$PATH

# gcloud + kubectl + gke-gcloud-auth-plugin from Google's apt repo.
RUN curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y --no-install-recommends \
        google-cloud-cli \
        google-cloud-cli-gke-gcloud-auth-plugin \
        kubectl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/automation

COPY requirements.txt /opt/automation/requirements.txt
RUN python3 -m venv /opt/automation/venv \
 && /opt/automation/venv/bin/pip install --no-cache-dir -r /opt/automation/requirements.txt
ENV PATH=/opt/automation/venv/bin:$PATH

COPY orchestrator.py /opt/automation/orchestrator.py
COPY manifests/runner-job.yaml.tmpl /opt/automation/manifests/runner-job.yaml.tmpl

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python3", "/opt/automation/orchestrator.py"]
