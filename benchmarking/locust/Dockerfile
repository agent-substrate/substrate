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

# Build context must be the monorepo root so the Go builder can pull in
# cmd/, internal/, pkg/, and vendor/. build_and_push.sh sets this up.

# Stage 1: Python dependency builder
FROM python:3.11-slim AS pydeps
WORKDIR /app
COPY benchmarking/locust/requirements.txt .
RUN pip install --no-cache-dir --target=/app/deps -r requirements.txt

# Stage 2: Go build of the boomer-glutton worker. Lives in the same image so
# runner.py can spawn it as a subprocess during headless runs of glutton.
# CGO_ENABLED=0 keeps us on boomer's pure-Go ZMQ (gomq), no libzmq needed.
FROM golang:1.26-bookworm AS goboomer
WORKDIR /src
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 GOFLAGS=-mod=vendor go build -o /out/boomer-glutton ./cmd/benchmarking/boomer-glutton

# Stage 3: Distroless runner. Carries the locust install AND the boomer
# binary; the manifest decides which to launch via the `args` field.
FROM gcr.io/distroless/python3-debian12
WORKDIR /app
COPY --from=pydeps /app/deps /app/deps
COPY --from=goboomer /out/boomer-glutton /app/boomer-glutton
COPY benchmarking/locust/common/ /app/common/
COPY benchmarking/locust/shapes/ /app/shapes/
COPY benchmarking/locust/tests/ /app/tests/
COPY benchmarking/locust/runner.py /app/runner.py

ENV PYTHONPATH=/app:/app/deps
ENV PYTHONUNBUFFERED=1
