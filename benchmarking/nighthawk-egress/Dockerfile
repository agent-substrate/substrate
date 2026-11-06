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

# Nighthawk egress-capacity benchmark runner image. Build context is the
# repo root. Always build with --platform linux/amd64
# (envoyproxy/nighthawk-dev publishes no arm64 images):
#   docker build --platform linux/amd64 -f benchmarking/nighthawk-egress/Dockerfile .

ARG NIGHTHAWK_COMMIT=21f3f100b86b11e5211e5672ade956c87baaa75c
ARG ENVOY_COMMIT=ad01ae36a702169140334ebadb2d0fc3be56327e
ARG NIGHTHAWK_IMAGE_DIGEST=sha256:7729902afea9d83f593b240690ba7d6e8fff04f6215ada538cfb4c1452b2553e

# --- Stage 1: proto descriptor set -----------------------------------------
FROM python:3.12-slim@sha256:b18b261e1b4974b4f321451ad5251ded85abb22c2b8b262da527dda1d031ab30 AS protogen
ARG NIGHTHAWK_COMMIT
ARG ENVOY_COMMIT
COPY benchmarking/locust/codegen/requirements.txt /tmp/codegen-requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r /tmp/codegen-requirements.txt

RUN git clone --depth 1 https://github.com/cncf/xds /src/xds \
    && git clone --depth 1 https://github.com/bufbuild/protoc-gen-validate /src/pgv \
    && git clone --depth 1 https://github.com/googleapis/googleapis /src/googleapis
RUN git init /src/nighthawk \
    && git -C /src/nighthawk fetch --depth 1 https://github.com/envoyproxy/nighthawk "${NIGHTHAWK_COMMIT}" \
    && git -C /src/nighthawk checkout FETCH_HEAD
RUN git init /src/envoy \
    && git -C /src/envoy fetch --depth 1 https://github.com/envoyproxy/envoy "${ENVOY_COMMIT}" \
    && git -C /src/envoy checkout FETCH_HEAD

COPY benchmarking/nighthawk-ingress/protogen/generate_descriptor.sh /gen.sh
RUN bash /gen.sh /out/nighthawk.desc

# --- Stage 2: runtime -------------------------------------------------------
FROM envoyproxy/nighthawk-dev@${NIGHTHAWK_IMAGE_DIGEST}

COPY benchmarking/nighthawk-egress/requirements.txt /tmp/requirements.txt
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip ca-certificates \
    && python3 -m pip install --no-cache-dir --break-system-packages \
        --target=/app/deps -r /tmp/requirements.txt \
    && apt-get purge -y python3-pip \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /tmp/requirements.txt

WORKDIR /app
COPY --from=protogen /out/nighthawk.desc /app/nighthawk.desc
COPY benchmarking/nighthawk_common /app/nighthawk_common
COPY benchmarking/nighthawk-egress/runner.py \
     benchmarking/nighthawk-egress/spec.py \
     benchmarking/nighthawk-egress/output.py \
     /app/
COPY benchmarking/nighthawk-egress/tests /app/tests

ENV PYTHONPATH=/app:/app/deps
ENV PYTHONUNBUFFERED=1
ENV NIGHTHAWK_DESC=/app/nighthawk.desc

RUN python3 /app/tests/check_descriptor.py \
    && python3 -m unittest discover -s /app/tests
USER 65532:65532
ENTRYPOINT ["python3", "/app/runner.py"]
