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


# The harness's one program, built outside, statically for linux, as
# run-docker.sh does:
#
#   CGO_ENABLED=0 GOOS=linux go build -o ctx/ ./benchmarking/envoy-sockmem
#   docker build -f benchmarking/envoy-sockmem/Dockerfile ctx/
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

COPY envoy-sockmem /usr/local/bin/
