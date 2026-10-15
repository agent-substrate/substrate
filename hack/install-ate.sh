#!/usr/bin/env bash

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
#
# Installs Agent Substrate.
#
# This is a translation shim: cmd/ate-setup carries out every action, and this
# script only maps the flags and environment variables the installer has always
# accepted onto its commands, so existing command lines and CI jobs keep
# working unchanged. It deliberately holds no install logic of its own.
#
# New work belongs in cmd/ate-setup. See cmd/ate-setup/commands.md for the
# equivalent invocation of each flag below, and cmd/ate-setup/cli-diff.md for
# the behaviors that differ after the move.
#
# The environment variables the installer reads -- BUCKET_NAME, KO_DOCKER_REPO,
# PROJECT_ID, CLUSTER_NAME, CLUSTER_LOCATION, KUBECTL_CONTEXT, NO_DEV_ENV, the
# ATE_API_POSTGRES_* set, and the rest -- are read by ate-setup directly, as is
# .ate-dev-env.sh. They need no translation and are not repeated here.

set -o errexit -o nounset -o pipefail

go run ./cmd/ate-setup "$@"
