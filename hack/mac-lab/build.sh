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
source "$(dirname "$0")/runtime.sh"
export PATH="$ROOT/.amp/in/toolchains/go/bin:/opt/homebrew/bin:$PATH"
[[ $(uname -sm) = 'Darwin arm64' ]]
if launchctl print "gui/$(id -u)/dev.substrate.mac-lab" >/dev/null 2>&1; then
  echo 'Stop the lab before rebuilding native binaries.' >&2; exit 1
fi
git -C "$ROOT" check-ignore "$LAB/runtime" >/dev/null || {
  echo 'Add /.amp/in/ to git info/exclude before building.' >&2; exit 1;
}
umask 077
mkdir -p "$LAB/downloads" "$LAB/logs" "$LAB/stage/bin"
if [[ ! -x "$CONTAINER" ]]; then
  pkg="$LAB/downloads/container-1.5.0.pkg"
  curl -fL https://github.com/apple/container/releases/download/1.5.0/container-1.5.0-installer-signed.pkg -o "$pkg"
  echo "a24808cb202318fa1c3bbee0c6c6887fe1225fe899d7b687a0ddd939bd6573f8  $pkg" | shasum -a 256 -c -
  pkgutil --check-signature "$pkg"
  expanded=$(mktemp -d "$LAB/downloads/extract.XXXXXX")
  rmdir "$expanded"
  pkgutil --expand-full "$pkg" "$expanded"
  mv "$expanded/Payload" "$LAB/dist"
  rm -rf "$expanded"
fi
# Never replace a running user's container system. The container service name
# is per-user, even though the lab's data and executables have private roots.
if "$CONTAINER" system status | grep -q 'status *running'; then
  "$CONTAINER" system status | grep -F "$CONTAINER_APP_ROOT/" >/dev/null || {
    echo 'Another container runtime is running; stop it yourself before using this lab.' >&2; exit 1;
  }
else
  "$CONTAINER" system start --app-root "$CONTAINER_APP_ROOT" --install-root "$CONTAINER_INSTALL_ROOT" \
    --log-root "$LAB/logs" --enable-kernel-install
fi
"$CONTAINER" build -t substrate-mac-lab:local "$ROOT/hack/mac-lab"
"$CONTAINER" builder stop
if ! "$CONTAINER" machine list --quiet | grep -qx substrate-lab; then
  "$CONTAINER" machine create --name substrate-lab --cpus 4 --memory 8G --home-mount none substrate-mac-lab:local
fi
for binary in ateapi atecontroller ate-setup; do
  CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go -C "$ROOT" build -o "$LAB/stage/bin/$binary" "./cmd/$binary"
done
go -C "$ROOT" build -o "$LAB/macletd" ./cmd/macletd
go -C "$ROOT/tools/mac-lab" build -o "$LAB/smoke" .
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go -C "$ROOT/tools/mac-lab" build -o "$LAB/stage/bin/mac-lab-smoke" .
swift build --package-path "$ROOT/cmd/maclet" -c release
maclet=$(swift build --package-path "$ROOT/cmd/maclet" -c release --show-bin-path)/maclet
codesign --force --sign - --entitlements "$ROOT/cmd/maclet/maclet.entitlements" "$maclet"
codesign --verify --strict "$maclet"
