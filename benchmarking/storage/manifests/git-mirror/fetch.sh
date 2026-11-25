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

# Fetches the git mirror's repositories from GitHub into /srv. A bench-done
# file marks each finished repository, so a restart skips it.
#
# - kubernetes.git and linux.git: full history. HEAD is pinned, so the checked
#   out tree is fixed. Other branches and tags are as of the fetch, so clone
#   size grows slowly with upstream.
# - linux-head.git: one commit holding the Linux tree at a pinned commit.

set -o errexit -o nounset

KUBERNETES_COMMIT=70cf7a0260ffbbedd79d283b29caeeb78e750882
LINUX_COMMIT=4405c18c95d2d0f99f6c581ef0ee8591a1aa6ccd
LINUX_HEAD_COMMIT=666539eecdf4db623f5e3c3e55cf9f2dfad24aa4

apt-get update -qq
apt-get install -y -qq git ca-certificates > /dev/null

# full NAME GITHUB_REPO COMMIT
full() {
  if [ ! -f "/srv/$1.git/bench-done" ]; then
    rm -rf "/srv/$1.git"
    git clone -q --bare "https://github.com/$2" "/srv/$1.git"
    git -C "/srv/$1.git" update-ref refs/heads/bench "$3"
    git -C "/srv/$1.git" symbolic-ref HEAD refs/heads/bench
    git -C "/srv/$1.git" repack -adbq
    git -C "/srv/$1.git" update-server-info
    touch "/srv/$1.git/bench-done"
  fi
}
full kubernetes kubernetes/kubernetes "${KUBERNETES_COMMIT}"
full linux torvalds/linux "${LINUX_COMMIT}"

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
du -sh /srv/*.git
