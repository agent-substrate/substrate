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

# One image holding the benchmark's one program; each container picks its
# subcommand. The program is built outside, statically for linux, so the build
# context is just it (run-docker.sh does this):
#
#   CGO_ENABLED=0 GOOS=linux go build -o ctx/ ./benchmarking/atunnel/cmd/...
#   docker build -f benchmarking/atunnel/Dockerfile ctx/
FROM gcr.io/distroless/static-debian12:nonroot

COPY atunnel-bench /usr/local/bin/
