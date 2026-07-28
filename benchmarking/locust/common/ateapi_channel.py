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

"""Authenticated gRPC channel to ateapi, shared by every locust user class.

ateapi rejects calls that carry no credential: its interceptor takes identity
from an mTLS client certificate, or failing that from an `authorization:
Bearer <jwt>` header. We present the certificate, which is what the base
install gives ate-controller and atenet-router.

Both files are projected in by the Deployment
(benchmarking/locust/manifests/locust.yaml).
"""

import re

import grpc

CA_FILE = "/run/servicedns-ca/ca.crt"
CRED_BUNDLE = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"

# The DNS SAN on the apiserver's serving cert, which is shorter than the
# endpoint the user classes dial.
SERVER_NAME = "api.ate-system.svc"

_PEM_BLOCK = re.compile(
    rb"-----BEGIN (?P<kind>[A-Z ]+)-----.*?-----END (?P=kind)-----\n?",
    re.DOTALL,
)


def _split_cred_bundle(bundle: bytes):
    """Split a Kubernetes pod-certificate bundle into (key, chain).

    The bundle is one file holding a PRIVATE KEY block followed by
    CERTIFICATE blocks in leaf-to-root order. grpc wants the two halves
    separately, so pull the PEM blocks apart by armor rather than parsing
    the DER — no crypto library needed, and nothing here has to understand
    the key type.
    """
    key, chain = None, []
    for m in _PEM_BLOCK.finditer(bundle):
        if m.group("kind") == b"PRIVATE KEY":
            key = m.group(0)
        else:
            chain.append(m.group(0))
    if key is None:
        raise ValueError(f"{CRED_BUNDLE}: no PRIVATE KEY block")
    if not chain:
        raise ValueError(f"{CRED_BUNDLE}: no CERTIFICATE block")
    return key, b"".join(chain)


def ateapi_channel(host: str, options=None) -> grpc.Channel:
    """Open an mTLS channel to ateapi that authenticates as this pod.

    The certificate is read once, when the channel is built. Python's gRPC
    has no per-handshake reload hook, unlike the Go client's
    credbundle.ClientLoader. That is fine in practice: each locust User
    builds its own channel in on_start, so a respawn picks up a rotated
    certificate, and an already-established connection is unaffected by the
    old one expiring.
    """
    target = host.replace("http://", "").replace("https://", "")
    with open(CA_FILE, "rb") as f:
        ca_cert = f.read()
    with open(CRED_BUNDLE, "rb") as f:
        private_key, cert_chain = _split_cred_bundle(f.read())

    creds = grpc.ssl_channel_credentials(
        root_certificates=ca_cert,
        private_key=private_key,
        certificate_chain=cert_chain,
    )
    channel_options = [("grpc.ssl_target_name_override", SERVER_NAME)]
    if options:
        channel_options.extend(options)
    return grpc.secure_channel(target, creds, options=channel_options)
