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

# Stage 1: Dependency builder
FROM python:3.11-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --target=/app/deps -r requirements.txt

# Stage 2: Distroless runner
FROM gcr.io/distroless/python3-debian12
WORKDIR /app
# Copy the dependencies from builder
COPY --from=builder /app/deps /app/deps
# Copy the locust files
COPY common/ /app/common/
COPY shapes/ /app/shapes/
COPY tests/ /app/tests/

# Set PYTHONPATH to include /app so modules can find each other
ENV PYTHONPATH=/app:/app/deps
ENV PYTHONUNBUFFERED=1


