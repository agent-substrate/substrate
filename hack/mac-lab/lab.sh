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
export PATH="$ROOT/.amp/in/toolchains/go/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
label=dev.substrate.mac-lab
domain="gui/$(id -u)"
umask 077
mkdir -p "$LAB/logs"
case ${1:-health} in
  deploy)
    if launchctl print "$domain/$label" >/dev/null 2>&1; then
      echo 'Stop the lab before redeploying binaries.' >&2; exit 1
    fi
    : "${MAC_LAB_SOURCE:?set MAC_LAB_SOURCE to a powered-off disposable fixture bundle}"
    : "${MAC_LAB_IMAGE:?set MAC_LAB_IMAGE to its digest-pinned reference}"
    test -d "$MAC_LAB_SOURCE"
    machine true
    gateway=$(machine ip -4 route | awk '/^default/ {print $3; exit}')
    ifconfig | grep -F "inet $gateway " >/dev/null
    bash "$ROOT/hack/mac-lab/pki.sh" "$LAB/pki" "$gateway"
    mkdir -p "$LAB/stage/tls" "$LAB/stage/hack" "$LAB/stage/manifests/ate-install" "$LAB/actors"
    for name in server-bundle.pem client-bundle.pem ca.crt host-client.crt host-client.key host-ca.crt; do
      cp "$LAB/pki/$name" "$LAB/stage/tls/$name"
    done
    cp -R "$ROOT/hack/mac-lab" "$ROOT/hack/obelisk" "$LAB/stage/hack/"
    cp -R "$ROOT/manifests/ate-install/generated" "$LAB/stage/manifests/ate-install/"
    sed "s/HOST_GATEWAY/$gateway/g" "$ROOT/hack/mac-lab/patches.yaml" > "$LAB/stage/hack/mac-lab/patches.yaml"
    cp "$ROOT/go.mod" "$LAB/stage/go.mod"
    touch "$LAB/stage/mac-lab-marker"
    machine mkdir -p /opt/substrate
    # Replace executable inodes, not their contents; existing pods can still
    # have the old binaries mapped. Omit directory entries from unlink-first.
    (cd "$LAB/stage"; find . -type f -print0 | tar --no-xattrs --null -T - -cf -) |
      machine tar --unlink-first -C /opt/substrate -xf -
    machine bash /opt/substrate/hack/mac-lab/deploy.sh
    printf 'GATEWAY=%q\nSOURCE=%q\nIMAGE=%q\n' "$gateway" "$MAC_LAB_SOURCE" "$MAC_LAB_IMAGE" > "$LAB/host.env"
    ;;
  start)
    test -f "$LAB/host.env"
    if launchctl print "$domain/$label" >/dev/null 2>&1; then exit 0; fi
    # plutil handles XML escaping of paths rather than interpolating them.
    plist="$LAB/supervisor.plist"
    plutil -create xml1 "$plist"
    plutil -insert Label -string "$label" "$plist"
    plutil -insert ProgramArguments -array "$plist"
    plutil -insert ProgramArguments.0 -string /bin/bash "$plist"
    plutil -insert ProgramArguments.1 -string "$ROOT/hack/mac-lab/lab.sh" "$plist"
    plutil -insert ProgramArguments.2 -string supervise "$plist"
    plutil -insert KeepAlive -bool YES "$plist"
    plutil -insert ThrottleInterval -integer 10 "$plist"
    plutil -insert StandardOutPath -string "$LAB/logs/supervisor.log" "$plist"
    plutil -insert StandardErrorPath -string "$LAB/logs/supervisor.log" "$plist"
    launchctl bootstrap "$domain" "$plist"
    ;;
  supervise)
    source "$LAB/host.env"
    children=()
    cleanup() { if ((${#children[@]})); then kill "${children[@]}" 2>/dev/null || true; wait || true; fi; }
    trap cleanup EXIT
    trap 'exit 0' TERM INT
    machine true
    current=$(machine ip -4 route | awk '/^default/ {print $3; exit}')
    [[ "$current" = "$GATEWAY" ]] || { echo 'Gateway changed; redeploy required.' >&2; exit 1; }
    ip=$("$CONTAINER" machine inspect substrate-lab | /usr/bin/plutil -extract 0.ipAddress raw -o - -)
    OTEL_METRICS_EXPORTER=none OTEL_TRACES_EXPORTER=none "$LAB/macletd" --listen-address="$GATEWAY:9443" \
      --maclet-executable="$ROOT/cmd/maclet/.build/out/Products/Release/maclet" \
      --state-directory="$LAB/actors" --source-bundle="$SOURCE" --image="$IMAGE" \
      --advertise-host="$GATEWAY" --proxy-listen-address="$GATEWAY:0" \
      --tls-cert-file="$LAB/pki/host-server.crt" --tls-key-file="$LAB/pki/host-server.key" \
      --client-ca-file="$LAB/pki/host-ca.crt" --metrics-listen-address=127.0.0.1:19090 \
      >> "$LAB/logs/macletd.log" 2>&1 &
    children+=("$!")
    for forward in 18443:30443 16443:6443; do
      socat "TCP4-LISTEN:${forward%:*},bind=127.0.0.1,reuseaddr,fork" "TCP4:$ip:${forward#*:}" &
      children+=("$!")
    done
    while sleep 5; do
      for pid in "${children[@]}"; do kill -0 "$pid"; done
      machine true
      current=$("$CONTAINER" machine inspect substrate-lab | /usr/bin/plutil -extract 0.ipAddress raw -o - -)
      [[ "$current" = "$ip" ]] || exit 1
    done
    ;;
  health)
    launchctl print "$domain/$label" | grep -E 'state =|pid ='
    curl -fsS http://127.0.0.1:19090/healthz
    machine k3s kubectl get --raw=/readyz
    machine k3s kubectl -n ate-system get pods,pvc
    machine k3s kubectl -n ate-system rollout status deployment/ate-api-server --timeout=30s
    machine k3s kubectl -n ate-system rollout status deployment/ate-controller --timeout=30s
    ;;
  stop)
    # Stop Actors through the Control API before stopping their host supervisor.
    if find "$LAB/actors" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
      echo 'Actor bundles remain; delete lab Actors through the API before stopping.' >&2; exit 1
    fi
    if launchctl print "$domain/$label" >/dev/null 2>&1; then launchctl bootout "$domain/$label"; fi
    "$CONTAINER" machine stop substrate-lab
    ;;
  *) echo 'usage: lab.sh deploy|start|health|stop' >&2; exit 2 ;;
esac
