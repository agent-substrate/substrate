#!/bin/sh

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

set -eu
dir=$(dirname "$0")
while [ "$1" != "-root" ]; do shift; done
shift 2
printf '%s\n' "$*" >> "$dir/calls"
if [ -f "$dir/$1.error" ]; then cat "$dir/$1.error" >&2; exit 1; fi
if [ -p "$dir/$1.block" ]; then touch "$dir/$1.entered"; exec cat "$dir/$1.block"; fi
case "$1" in
pause)
  for name in app sidecar; do
    if [ -f "$dir/$name.after-pause" ]; then cp "$dir/$name.after-pause" "$dir/$name.state"; fi
  done ;;
state)
  cat "$dir/$2.state" ;;
wait) cat "$dir/$2.wait" ;;
resume)
  if [ -f "$dir/checkpoint.saved" ]; then printf 'exit\n' > "$dir/sandbox.exit"; fi ;;
checkpoint)
  img=""
  while [ "$#" -gt 0 ]; do [ "$1" = "-image-path" ] && { img=$2; break; }; shift; done
  mkdir -p "$img"; touch "$img/checkpoint.img" "$dir/checkpoint.saved" ;;
fscheckpoint)
  img=""
  while [ "$#" -gt 0 ]; do [ "$1" = "-image-path" ] && { img=$2; break; }; shift; done
  mkdir -p "$img/fs"; touch "$img/fs/fscheckpoint.pb" ;;
esac
