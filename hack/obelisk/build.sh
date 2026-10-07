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

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
GO_VERSION=1.27.0
GO_SHA256=675c26c449cbb18fc24b74650de1eabbae6e16f64326fd85a283fb3b58280685
GO_ROOT="${HOME}/.local/go${GO_VERSION}"
K3S_VERSION='v1.36.5+k3s1'
K3S_SHA256=d73847bcd3c5fccef0115b372e2f9a91f3032dc84bbf71518a4617565294d313
TAILSCALE_VERSION=1.102.5
TAILSCALE_SHA256=65e6d7f19ad7e1c87d20c2a21e92f38a96795cb897af54b04536590e1c148d12

install_go() {
  if [[ -x "${GO_ROOT}/bin/go" ]]; then
    return
  fi
  local archive
  archive="$(mktemp)"
  trap 'rm -f "${archive}"' RETURN
  curl --fail --location --silent --show-error \
    "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" >"${archive}"
  echo "${GO_SHA256}  ${archive}" | sha256sum --check --status
  rm -rf "${GO_ROOT}"
  mkdir -p "${GO_ROOT}"
  tar -C "${GO_ROOT}" --strip-components=1 -xzf "${archive}"
}

install_tailscale() {
  if [[ "$(/usr/local/bin/tailscale version 2>/dev/null | head -1 || true)" != "${TAILSCALE_VERSION}" ]]; then
    local archive temp_dir
    temp_dir="$(mktemp --directory)"
    archive="${temp_dir}/tailscale.tgz"
    trap 'rm -rf "${temp_dir}"' RETURN
    curl --fail --location --silent --show-error \
      "https://pkgs.tailscale.com/stable/tailscale_${TAILSCALE_VERSION}_amd64.tgz" >"${archive}"
    echo "${TAILSCALE_SHA256}  ${archive}" | sha256sum --check --status
    tar -C "${temp_dir}" -xzf "${archive}"
    sudo install -m 0755 \
      "${temp_dir}/tailscale_${TAILSCALE_VERSION}_amd64/tailscale" \
      "${temp_dir}/tailscale_${TAILSCALE_VERSION}_amd64/tailscaled" \
      /usr/local/bin/
    rm -rf "${temp_dir}"
    trap - RETURN
  fi

  local unit
  unit="$(mktemp)"
  trap 'rm -f "${unit}"' RETURN
  cat >"${unit}" <<'EOF'
[Unit]
Description=Tailscale node agent
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/tailscaled --state=/var/lib/tailscale/tailscaled.state --socket=/run/tailscale/tailscaled.sock --port=41641
Restart=on-failure
RestartSec=5s
StateDirectory=tailscale
StateDirectoryMode=0700
RuntimeDirectory=tailscale
RuntimeDirectoryMode=0755
UMask=0077

[Install]
WantedBy=multi-user.target
EOF
  if ! sudo cmp --silent "${unit}" /etc/systemd/system/tailscaled.service 2>/dev/null; then
    sudo install -m 0644 "${unit}" /etc/systemd/system/tailscaled.service
    sudo systemctl daemon-reload
  fi
  sudo systemctl enable tailscaled.service
  sudo systemctl restart tailscaled.service
}

install_k3s() {
  if [[ "$(/usr/local/bin/k3s --version 2>/dev/null | head -1 || true)" != *"${K3S_VERSION}"* ]]; then
    local binary
    binary="$(mktemp)"
    trap 'rm -f "${binary}"' RETURN
    curl --fail --location --silent --show-error \
      "https://github.com/k3s-io/k3s/releases/download/${K3S_VERSION/+/%2B}/k3s" >"${binary}"
    echo "${K3S_SHA256}  ${binary}" | sha256sum --check --status
    chmod 0755 "${binary}"
    sudo install -m 0755 "${binary}" /usr/local/bin/k3s
  fi

  local unit
  unit="$(mktemp)"
  trap 'rm -f "${unit}"' RETURN
  cat >"${unit}" <<'EOF'
[Unit]
Description=Experimental Substrate Lab Kubernetes
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
KillMode=process
Delegate=yes
LimitNOFILE=1048576
LimitNPROC=infinity
LimitCORE=infinity
TasksMax=infinity
Restart=always
RestartSec=5s
ExecStartPre=-/usr/bin/ip link add substrate0 type dummy
ExecStartPre=/usr/bin/ip address replace 10.254.0.1/32 dev substrate0
ExecStartPre=/usr/bin/ip link set substrate0 up
ExecStart=/usr/local/bin/k3s server --node-ip=10.254.0.1 --advertise-address=10.254.0.1 --flannel-iface=eth0 --disable=traefik --disable=servicelb --disable=metrics-server --write-kubeconfig-mode=600 --kube-apiserver-arg=feature-gates=ClusterTrustBundle=true,ClusterTrustBundleProjection=true,PodCertificateRequest=true --kube-apiserver-arg=runtime-config=certificates.k8s.io/v1beta1=true --kubelet-arg=feature-gates=ClusterTrustBundle=true,ClusterTrustBundleProjection=true,PodCertificateRequest=true

[Install]
WantedBy=multi-user.target
EOF
  if ! sudo cmp --silent "${unit}" /etc/systemd/system/k3s.service 2>/dev/null; then
    sudo install -m 0644 "${unit}" /etc/systemd/system/k3s.service
    sudo systemctl daemon-reload
    sudo systemctl enable k3s.service
    sudo systemctl restart k3s.service
  elif ! sudo systemctl is-active --quiet k3s.service; then
    sudo systemctl start k3s.service
  fi
}

install_go
install_tailscale
install_k3s

export PATH="${GO_ROOT}/bin:${PATH}"
mkdir -p "${ROOT}/bin"
CGO_ENABLED=0 go build -mod=vendor -trimpath -o "${ROOT}/bin/ateapi" ./cmd/ateapi
CGO_ENABLED=0 go build -mod=vendor -trimpath -o "${ROOT}/bin/atecontroller" ./cmd/atecontroller
CGO_ENABLED=0 go build -mod=vendor -trimpath -o "${ROOT}/bin/ate-setup" ./cmd/ate-setup
