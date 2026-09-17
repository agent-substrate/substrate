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
# This is sourced as part of install-ate.sh. Do not run directly.
#
# --experimental-egress-credential-injection turns on egress credential
# injection on the sdsmint egress gateway's decrypted MITM leg. The injection is
# performed by the egress gateway's own co-located atenet router sidecar, which
# the MITM leg already calls over ext_proc to enforce the actor's EgressPolicy;
# turning injection on is only a matter of pointing that sidecar at a credential
# provider. Enabling it splices the --credential-provider-* flags into the
# sidecar over the #ATE_EGRESS_INJECT_FLAGS marker in
# atenet-egress-with-sdsmint.yaml.

egress_credential_injection_enabled() {
  [[ "${ATE_CREDENTIAL_INJECTION_ENABLED:-false}" == "true" ]]
}

# Note: the heredoc is written at column zero; the awk in
# patch_atenet_egress_inject re-indents to the marker's column.

# Arguments:
#
# $1 = credential-provider name (ate-secret:// prefix)
# $2 = credential-provider gRPC address
# $3 = credential-provider serving-cert SAN to pin
emit_egress_inject_flags() {
  local name="$1" address="$2" server_name="$3"
  cat <<EOF
# Added by hack/install-ate.sh --experimental-egress-credential-injection.
# Spliced over the #ATE_EGRESS_INJECT_FLAGS marker. Points the egress handler at
# the credential provider over mTLS (presenting podidentity, validating the
# provider's servicedns serving cert). The volumes are already mounted for the
# ateapi client above.
- --credential-provider-name=${name}
- --credential-provider-address=${address}
- --credential-provider-ca-file=/run/servicedns.podcert.ate.dev/trust-bundle.pem
- --credential-provider-client-cert=/run/podidentity.podcert.ate.dev/credential-bundle.pem
- --credential-provider-server-name=${server_name}
EOF
}

# patch_atenet_egress_inject reads the egress manifest on stdin and writes it to
# stdout with the #ATE_EGRESS_INJECT_FLAGS marker replaced by the injection
# handler's credential-provider flags.
#
# It reads stdin (rather than the manifest file directly) so it composes after
# the general --experimental-additional-egress-extproc-service patch when both
# are enabled: patch_atenet_egress_manifest | patch_atenet_egress_inject.
patch_atenet_egress_inject() {
  # Only the sdsmint manifest carries the marker. Refuse rather than emit an
  # unpatched manifest: silently ignoring the flag would deploy a gateway with
  # no injector while the install reported success.
  if [[ "${ATE_EXPERIMENTAL_USE_SDSMINT:-false}" != "true" ]]; then
    echo "Error: --experimental-egress-credential-injection requires --experimental-use-sdsmint" >&2
    return 1
  fi

  local name="${ATE_CREDENTIAL_PROVIDER_NAME:-ate-secret://kubernetes.io}"
  local address="${ATE_CREDENTIAL_PROVIDER_ADDRESS:-credprovider.ate-system.svc:50051}"
  # Pin the provider's serving-cert SAN to its Service DNS name (the address
  # without the port), so a rotated cert for the same Service still validates.
  local server_name="${address%:*}"

  local flags_block
  flags_block="$(emit_egress_inject_flags "${name}" "${address}" "${server_name}")" || return 1

  # Anchored to the start of the line so that prose mentioning the marker is not
  # itself replaced by a config block.
  ATE_EXTPROC_FLAGS_BLOCK="${flags_block}" \
  awk '
    BEGIN { flags = ENVIRON["ATE_EXTPROC_FLAGS_BLOCK"] }

    /^[ \t]*#ATE_EGRESS_INJECT_FLAGS/ { splice(flags); flagsets++; next }
    { print }

    END {
      if (flagsets != 1) {
        printf("Error: expected 1 #ATE_EGRESS_INJECT_FLAGS marker, found %d\n", flagsets) > "/dev/stderr"
        exit 1
      }
    }

    # Prints block at the indentation of the marker line being replaced. The
    # heredoc above is written at column zero.
    function splice(block, indent, lines, n, i) {
      match($0, /^[ \t]*/)
      indent = substr($0, 1, RLENGTH)
      n = split(block, lines, "\n")
      for (i = 1; i <= n; i++) {
        print (lines[i] == "" ? "" : indent lines[i])
      }
    }
  '
}
