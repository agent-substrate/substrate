#  Copyright 2026 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

FROM debian:bookworm-slim

ARG ANTIGRAVITY_VERSION=1.23.2-1776332190

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
  && install -d -m 0755 /etc/apt/keyrings \
  && curl -fsSL https://us-central1-apt.pkg.dev/doc/repo-signing-key.gpg \
    | gpg --dearmor --yes -o /etc/apt/keyrings/antigravity-repo-key.gpg \
  && printf '%s\n' 'deb [signed-by=/etc/apt/keyrings/antigravity-repo-key.gpg] https://us-central1-apt.pkg.dev/projects/antigravity-auto-updater-dev/ antigravity-debian main' \
    > /etc/apt/sources.list.d/antigravity.list \
  && apt-get update \
  && apt-get install -y --no-install-recommends "antigravity=${ANTIGRAVITY_VERSION}" \
  && rm -rf /var/lib/apt/lists/*

COPY run.sh /run.sh
RUN chmod +x /run.sh

RUN useradd --create-home --shell /bin/bash antigravity
USER antigravity
WORKDIR /home/antigravity

ENV TASK="Tell me a fact about the moon."
ENV INTERVAL_SECONDS=60
ENV ANTIGRAVITY_COMMAND="antigravity --help"

ENTRYPOINT ["/run.sh"]
