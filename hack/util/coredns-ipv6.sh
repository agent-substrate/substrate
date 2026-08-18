#!/usr/bin/env bash

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

# The CoreDNS Corefile rewrite that IPv6-only kind clusters need, kept in a pure
# function so hack/verify-ipv6-dns.sh can exercise it without building a cluster.

# coredns_ipv6_corefile <corefile> <registry-address> <registry-name> <upstream>
#
# Echoes <corefile> with the resolv.conf forwarder replaced by a hosts block for
# the registry plus a forwarder to <upstream>; returns 1 if the search string is
# absent. The search is a prefix of kind's block form, so options the block
# carries (max_concurrent) re-attach to the new forwarder.
coredns_ipv6_corefile() {
  local corefile="$1" reg_addr="$2" reg_name="$3" upstream="$4"

  local search="forward . /etc/resolv.conf"
  # fallthrough is load-bearing: without it every name but the registry NXDOMAINs.
  local replace="hosts {
       ${reg_addr} ${reg_name}
       fallthrough
    }
    forward . ${upstream}"

  # Both sides unquoted -- bash 3.2 would splice the quotes in literally.
  local patched="${corefile/$search/$replace}"
  if [[ "${patched}" == "${corefile}" ]]; then
    echo "error: '${search}' not found in the CoreDNS Corefile" >&2
    echo "       a silent no-op here is the whole failure mode; inspect it by hand" >&2
    return 1
  fi

  printf '%s\n' "${patched}"
}
