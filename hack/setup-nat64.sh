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

set -o errexit -o nounset -o pipefail

# The well-known prefix, which is also what CoreDNS's dns64 plugin defaults to.
PREFIX="64:ff9b::/96"
DEV="nat64"
PROBE_HOST="${NAT64_PROBE_HOST:-storage.googleapis.com}"

if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help)
      echo "Usage: $0"
      echo "Sets up NAT64 (${PREFIX}) so an IPv6-only kind cluster can reach IPv4-only"
      echo "destinations. Linux only, and needs sudo. Safe to re-run."
      echo
      echo "Only needed where the host itself cannot reach the internet over IPv6, which"
      echo "is true of GitHub Actions runners and false of most Linux boxes and Lima VMs"
      echo "on a v6-capable network. The deciding test, which this script also runs:"
      echo
      echo "  curl -6 -sS -o /dev/null https://${PROBE_HOST}/ && echo 'no NAT64 needed'"
      echo
      echo "Pair it with IPV6_DNS64_PREFIX=${PREFIX} on hack/create-kind-cluster.sh."
      echo "That is the half that makes pods resolve names into the prefix; without it"
      echo "the translator sits there unused, and without the translator the synthesized"
      echo "addresses go nowhere. Neither is any use alone."
      echo
      echo "Configured through the environment:"
      echo "  NAT64_PROBE_HOST   Name the translation probe resolves and fetches"
      echo "                     (default: ${PROBE_HOST})."
      echo "  NAT64_FORCE        Set to run even where IPv6 egress already works, which this"
      echo "                     otherwise refuses to do -- enabling forwarding costs the host"
      echo "                     any IPv6 route it learned from a router advertisement."
      exit 0
      ;;
  esac
fi

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "error: tayga is Linux-only; run this inside the Linux VM hosting your Docker daemon" >&2
  exit 1
fi

# Refuse where NAT64 is not needed, because here it is not merely redundant.
# Turning on IPv6 forwarding below makes the kernel stop honouring router
# advertisements on every interface left at the default accept_ra=1, so on a
# host whose IPv6 default route came from an RA this takes that route out when
# it expires -- long after the run that caused it.
#
# Twice, and not on a short timeout. A host with no IPv6 egress fails this in
# milliseconds, so the retry costs CI nothing, while a cold TLS handshake on a
# host that does have egress can outrun a tight deadline -- and that misread is
# the one that does the damage.
if [[ -z "${NAT64_FORCE:-}" ]]; then
  for _ in 1 2; do
    if curl -6 -sS -m 10 -o /dev/null "https://${PROBE_HOST}/" 2>/dev/null; then
      echo "error: this host already reaches ${PROBE_HOST} over IPv6, so it does not need NAT64." >&2
      echo "       Create the cluster without IPV6_DNS64_PREFIX and external names resolve normally." >&2
      echo "       Set NAT64_FORCE=1 to override; see the accept_ra note in this script first." >&2
      exit 1
    fi
  done
fi

echo "Installing tayga..."
if ! command -v tayga >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq tayga
fi
# Debian's package starts a unit the moment apt finishes, so tayga is already
# running against the stock config before this writes its own. Take it over
# rather than reconfiguring around it: one instance, and a log to point at.
if systemctl cat tayga.service >/dev/null 2>&1; then
  sudo systemctl disable --now tayga.service >/dev/null 2>&1 || true
fi

# tayga answers to .1/::1 and the tun holds .2/::2, so host-originated traffic
# is not sourced from tayga's own address, which is self-addressed rather than
# translatable. The dynamic pool avoids both.
sudo tee /etc/tayga.conf >/dev/null <<EOF
tun-device ${DEV}
ipv4-addr 192.168.255.1
# tayga refuses the well-known prefix with an RFC1918 pool unless it also holds
# a v6 address of its own, outside that prefix.
ipv6-addr 2001:db8:64::1
prefix ${PREFIX}
dynamic-pool 192.168.255.128/25
data-dir /var/spool/tayga
EOF
sudo mkdir -p /var/spool/tayga

echo "Bringing up the ${DEV} device..."
ip link show "${DEV}" >/dev/null 2>&1 || sudo tayga --mktun
sudo ip link set "${DEV}" up
ip -4 addr show dev "${DEV}" | grep -q '192\.168\.255\.2' \
  || sudo ip addr add 192.168.255.2/24 dev "${DEV}"
ip -6 addr show dev "${DEV}" | grep -q '2001:db8:64::2' \
  || sudo ip -6 addr add 2001:db8:64::2/128 dev "${DEV}"
ip -6 route show "${PREFIX}" | grep -q . \
  || sudo ip -6 route add "${PREFIX}" dev "${DEV}" src 2001:db8:64::2

sudo sysctl -qw net.ipv4.ip_forward=1
sudo sysctl -qw net.ipv6.conf.all.forwarding=1

sudo iptables -t nat -C POSTROUTING -s 192.168.255.0/24 -j MASQUERADE 2>/dev/null \
  || sudo iptables -t nat -A POSTROUTING -s 192.168.255.0/24 -j MASQUERADE
# Insert, not append: docker sets the FORWARD policy to DROP. Re-run after a
# dockerd restart, which rebuilds the chains these rules live in.
for dir in -i -o; do
  sudo iptables -C FORWARD "${dir}" "${DEV}" -j ACCEPT 2>/dev/null \
    || sudo iptables -I FORWARD 1 "${dir}" "${DEV}" -j ACCEPT
  sudo ip6tables -C FORWARD "${dir}" "${DEV}" -j ACCEPT 2>/dev/null \
    || sudo ip6tables -I FORWARD 1 "${dir}" "${DEV}" -j ACCEPT
done

# Unconditionally ours, so a re-run picks up an edited config and the log below
# is never some earlier instance's.
sudo pkill -x tayga 2>/dev/null || true
# -d keeps tayga in the foreground and logs a reason for every packet it
# declines to translate; detaching hides the failures worth diagnosing.
sudo sh -c "nohup tayga -d --config /etc/tayga.conf >/tmp/tayga.log 2>&1 &"
sleep 3
pgrep -a tayga || {
  echo "error: tayga is not running" >&2
  sudo cat /tmp/tayga.log >&2 || true
  exit 1
}

# A cluster built on a broken translator takes minutes to fail and does it as a
# rollout timeout, so gate here where the message is unambiguous. Map a live A
# record rather than hardcoding one.
v4="$(getent ahostsv4 "${PROBE_HOST}" | awk 'NR==1{print $1}')"
if [[ -z "${v4}" ]]; then
  echo "error: cannot resolve an IPv4 address for ${PROBE_HOST}" >&2
  exit 1
fi
# shellcheck disable=SC2086
set -- ${v4//./ }
v6="$(printf '64:ff9b::%02x%02x:%02x%02x' "$1" "$2" "$3" "$4")"
echo "NAT64 maps ${v4} -> ${v6}"
# No `|| echo 000`: curl already writes 000 on a connect failure, and a second
# one appends rather than replaces.
code="$(curl -6 -sS -m 15 -o /dev/null -w '%{http_code}' \
  --resolve "${PROBE_HOST}:443:[${v6}]" "https://${PROBE_HOST}/" 2>/dev/null || true)"
case "${code}" in
  # Any HTTP status proves the translator carried a TCP stream; GCS answers a
  # bare / with 400. ICMP is blocked separately, so a ping here would report a
  # failure that does not matter.
  2*|3*|4*)
    echo "NAT64 is translating (HTTP ${code})"
    echo
    echo "Half of the setup. Cluster DNS has to point at the prefix too:"
    echo "  IP_FAMILY=ipv6 IPV6_DNS64_PREFIX=${PREFIX} $(dirname "$0")/create-kind-cluster.sh"
    ;;
  *)
    echo "error: NAT64 is not translating (HTTP ${code})" >&2
    sudo cat /tmp/tayga.log >&2 || true
    exit 1
    ;;
esac
