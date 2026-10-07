#!/bin/bash
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

set -euo pipefail
umask 077
dir=${1:?private PKI directory required}
gateway=${2:?private host gateway required}
mkdir -p "$dir"
cd "$dir"
if [[ -f complete ]]; then
  for cert in server client operator host-server host-client; do
    openssl x509 -checkend 3600 -noout -in "$cert.crt"
  done
  exit
fi
if [[ -e ca.key ]]; then echo 'Incomplete PKI; inspect it before retrying.' >&2; exit 1; fi
for ca in ca host-ca; do
  openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=substrate-mac-lab-$ca" \
    -keyout "$ca.key" -out "$ca.crt" >/dev/null 2>&1
done
issue() {
  local name=$1 ca=$2 san=$3 usage=$4
  openssl req -new -newkey rsa:2048 -nodes -subj "/CN=$name" -keyout "$name.key" -out "$name.csr" >/dev/null 2>&1
  printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=%s\nsubjectAltName=%s\n' "$usage" "$san" > "$name.ext"
  openssl x509 -req -in "$name.csr" -CA "$ca.crt" -CAkey "$ca.key" -CAcreateserial \
    -days 7 -extfile "$name.ext" -out "$name.crt" >/dev/null 2>&1
  cat "$name.crt" "$name.key" > "$name-bundle.pem"
  rm "$name.csr" "$name.ext"
}
issue server ca 'DNS:api.ate-system.svc,DNS:localhost,IP:127.0.0.1' serverAuth
issue client ca 'URI:spiffe://cluster.local/ns/ate-system/sa/ate-controller' clientAuth
issue operator ca 'URI:spiffe://mac-lab/operator' clientAuth
issue host-server host-ca "DNS:host.container.internal,IP:$gateway" serverAuth
issue host-client host-ca 'URI:spiffe://mac-lab/control-plane' clientAuth
touch complete
