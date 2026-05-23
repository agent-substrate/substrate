#!/usr/bin/env bash

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

set -o errexit -o nounset -o pipefail

# pin protoc version and binary hashes
PROTOC_VERSION="25.3"
linux_x86_64_EXPECTED_SHA="f853e691868d0557425ea290bf7ba6384eef2fa9b04c323afab49a770ba9da80"
linux_aarch_64_EXPECTED_SHA="9eae1f20f70cccc912d1c318c3929b86aebf5afd4b0f32c196ef682c222ed5ae"
osx_x86_64_EXPECTED_SHA="247e003b8e115405172eacc50bd19825209d85940728e766f0848eee7c80e2a1"
osx_aarch_64_EXPECTED_SHA="d0fcd6d3b3ef6f22f1c47cc30a80c06727e1eccdddcaf0f4a3be47c070ffd3fe"

# Determine OS and Arch for protoc release
# Standard releases: linux-x86_64, osx-x86_64, osx-aarch_64
OS_NAME=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH_NAME=$(uname -m)

if [ "$OS_NAME" = "darwin" ]; then
  if [ "$ARCH_NAME" = "x86_64" ]; then
    PROTOC_PLATFORM="osx-x86_64"
  else
    PROTOC_PLATFORM="osx-aarch_64"
  fi
else

  if [ "$ARCH_NAME" = "x86_64" ]; then
    PROTOC_PLATFORM="linux-x86_64"
  elif [ "$ARCH_NAME" = "aarch64" ] || [ "$ARCH_NAME" = "arm64" ]; then
    PROTOC_PLATFORM="linux-aarch_64"
  fi
fi

# Target folder for local protoc binary
OUT_DIR="$(dirname "$0")/../bin/protoc-install"
PROTOC_BIN="$OUT_DIR/bin/protoc"


if [ ! -f "$PROTOC_BIN" ]; then
  echo "Downloading protoc v${PROTOC_VERSION} for ${PROTOC_PLATFORM}..."
  mkdir -p "$OUT_DIR"
  URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-${PROTOC_PLATFORM}.zip"
  
  case "$PROTOC_PLATFORM" in
    "linux-x86_64") EXPECTED_SHA="${linux_x86_64_EXPECTED_SHA}";;
    "linux-aarch_64") EXPECTED_SHA="${linux_aarch_64_EXPECTED_SHA}";;
    "osx-x86_64") EXPECTED_SHA="${osx_x86_64_EXPECTED_SHA}";;
    "osx-aarch_64") EXPECTED_SHA="${osx_aarch_64_EXPECTED_SHA}";;
    *) echo "Unknown platform $PROTOC_PLATFORM"; exit 1 ;;
  esac

  # Download and verify
  curl -sSL "$URL" -o "$OUT_DIR/protoc.zip"
  ACTUAL_SHA=$(shasum -a 256 "$OUT_DIR/protoc.zip" | awk '{print $1}')
  if [ "$ACTUAL_SHA" != "$EXPECTED_SHA" ]; then
    echo "Checksum verification failed for protoc download!"
    echo "Expected: $EXPECTED_SHA"
    echo "Got:      $ACTUAL_SHA"
    exit 1
  fi

  unzip -q -o "$OUT_DIR/protoc.zip" -d "$OUT_DIR"
  rm -f "$OUT_DIR/protoc.zip"
  chmod +x "$PROTOC_BIN"
  echo "Local protoc installed at $PROTOC_BIN"
fi


exec "$PROTOC_BIN" "$@"
