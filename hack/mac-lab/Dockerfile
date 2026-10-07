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

# One persistent systemd machine, not a fleet of host-managed containers.
FROM ubuntu:24.04@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55
ENV container=container
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    systemd systemd-sysv dbus ca-certificates curl iproute2 iptables nftables \
    socat tar sudo openssl && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN curl -fL 'https://github.com/k3s-io/k3s/releases/download/v1.36.5%2Bk3s1/k3s-arm64' -o /usr/local/bin/k3s && \
    echo '135e34cb9e8a1cfae3cb55577789e93501efe7edc080a8b2630458a85b07721e  /usr/local/bin/k3s' | sha256sum -c - && \
    chmod 755 /usr/local/bin/k3s
COPY k3s.service /etc/systemd/system/k3s.service
COPY k3s-start.sh /usr/local/bin/k3s-start
RUN chmod 755 /usr/local/bin/k3s-start && \
    : > /etc/machine-id && : > /var/lib/dbus/machine-id && \
    systemctl set-default multi-user.target && \
    systemctl mask dev-hugepages.mount sys-fs-fuse-connections.mount \
      systemd-update-utmp.service systemd-tmpfiles-setup.service \
      console-getty.service getty@.service systemd-logind.service && \
    systemctl enable k3s.service
STOPSIGNAL SIGRTMIN+3
CMD ["/sbin/init"]
