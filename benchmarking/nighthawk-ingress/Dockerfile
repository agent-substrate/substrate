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

# Nighthawk router-capacity benchmark runner image. Build context is the
# repo root; always build with --platform linux/amd64
# (envoyproxy/nighthawk-dev publishes no arm64 images):
#   docker build --platform linux/amd64 -f benchmarking/nighthawk-ingress/Dockerfile .

# The three pins move together: NIGHTHAWK_IMAGE_DIGEST is the digest of the
# Docker Hub tag envoyproxy/nighthawk-dev:<NIGHTHAWK_COMMIT> (resolve with
# docker buildx imagetools inspect), and ENVOY_COMMIT is nighthawk's envoy
# pin from bazel/repositories.bzl at that commit.
ARG NIGHTHAWK_COMMIT=21f3f100b86b11e5211e5672ade956c87baaa75c
ARG ENVOY_COMMIT=ad01ae36a702169140334ebadb2d0fc3be56327e
ARG NIGHTHAWK_IMAGE_DIGEST=sha256:7729902afea9d83f593b240690ba7d6e8fff04f6215ada538cfb4c1452b2553e

# --- Stage 1: proto descriptor set -----------------------------------------
# nighthawk_service speaks protobuf: spec.py builds the adaptive-load session
# spec and output.py parses the result, and those message trees pull in types
# from envoy, xds, protoc-gen-validate and googleapis. This stage compiles
# that whole closure into one FileDescriptorSet, which spec.py/output.py load
# at runtime.
FROM python:3.12-slim AS protogen
ARG NIGHTHAWK_COMMIT
ARG ENVOY_COMMIT
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir grpcio-tools==1.81.1

# Shallow-fetch each tree at its pin. nighthawk/envoy are pinned to bare
# commits, which clone --branch can't fetch, hence init+fetch.
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
# nighthawk-dev has no stable tags; pinned by digest so the pull is
# immutable. Ships the nighthawk_* binaries in /usr/local/bin on ubuntu.
FROM envoyproxy/nighthawk-dev@${NIGHTHAWK_IMAGE_DIGEST}

# Deps must be installed with this image's own interpreter: grpcio/protobuf
# native extensions are python-version-specific.
COPY benchmarking/nighthawk-ingress/requirements.txt /tmp/requirements.txt
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip ca-certificates \
    && python3 -m pip install --no-cache-dir --break-system-packages \
        --target=/app/deps -r /tmp/requirements.txt \
    && apt-get purge -y python3-pip \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /tmp/requirements.txt

WORKDIR /app
COPY --from=protogen /out/nighthawk.desc /app/nighthawk.desc

# ateapi client reused from the locust harness (atespace.py excluded: it
# imports locust).
COPY benchmarking/locust/common/__init__.py /app/common/__init__.py
COPY benchmarking/locust/common/ateapi_channel.py /app/common/ateapi_channel.py
COPY benchmarking/locust/common/ateapi_pb2.py /app/common/ateapi_pb2.py
COPY benchmarking/locust/common/ateapi_pb2_grpc.py /app/common/ateapi_pb2_grpc.py

COPY benchmarking/nighthawk-ingress/runner.py \
     benchmarking/nighthawk-ingress/spec.py \
     benchmarking/nighthawk-ingress/output.py \
     benchmarking/nighthawk-ingress/actors.py \
     /app/
COPY benchmarking/nighthawk-ingress/tests /app/tests

ENV PYTHONPATH=/app:/app/deps
ENV PYTHONUNBUFFERED=1
ENV NIGHTHAWK_DESC=/app/nighthawk.desc

ENTRYPOINT ["python3", "/app/runner.py"]
