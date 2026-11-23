#!/bin/sh
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

# Fetches the git mirror's repository from GitHub into /srv: linux-head.git,
# one commit holding the Linux tree at a pinned commit. A bench-done file
# marks it finished, so a restart skips it.

set -o errexit -o nounset

LINUX_HEAD_COMMIT=666539eecdf4db623f5e3c3e55cf9f2dfad24aa4

apt-get update -qq
apt-get install -y -qq git ca-certificates > /dev/null

if [ ! -f /srv/linux-head.git/bench-done ]; then
  rm -rf /srv/src /srv/linux-head.git
  git init -q /srv/src
  git -C /srv/src fetch -q --depth 1 https://github.com/torvalds/linux "${LINUX_HEAD_COMMIT}"
  git -C /srv/src checkout -q FETCH_HEAD
  rm -rf /srv/src/.git
  git init -q -b master /srv/src
  git -C /srv/src add -A
  git -C /srv/src -c user.name=bench -c user.email=bench@example.com \
    commit -q -m "linux ${LINUX_HEAD_COMMIT}"
  git clone -q --bare /srv/src /srv/linux-head.git
  git -C /srv/linux-head.git repack -adq
  git -C /srv/linux-head.git update-server-info
  rm -rf /srv/src
  touch /srv/linux-head.git/bench-done
fi
du -sh /srv/linux-head.git
