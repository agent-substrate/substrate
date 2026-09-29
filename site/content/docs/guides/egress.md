---
title: "Egress"
linkTitle: "Egress"
weight: 5
repo_source: "docs/network-egress.md"
repo_sources:
  - "docs/egress-traffic.md"
  - "docs/network-egress.md"
  - "docs/egress-trust-bundle.md"
aliases:
  - "/docs/guides/egress-traffic/"
  - "/docs/guides/network-egress/"
  - "/docs/guides/egress-trust-bundle/"
description: >
  Configure outbound actor networking, supported egress protocols, mTLS policy enforcement, and TLS MITM trust bundles.
---

This guide consolidates Agent Substrate's outbound networking behavior into three sections:
1. **[Supported Egress Traffic](#1-supported-egress-traffic--protocols)** — Which TCP, UDP, and DNS flows are permitted or blocked.
2. **[Network Egress Contract](#2-network-egress-contract--mtls-tunneling)** — How `atunnel` and the egress policy enforcement point (PEP) authenticate and route outbound connections.
3. **[TLS MITM Interception & Trust Bundles](#3-enabling-tls-mitm-interception--trust-bundles)** — How to project the gateway's CA bundle (`egress-mitm.ate.dev`) into actors when running `sdsmint`.

---

## 1. Supported Egress Traffic & Protocols {#1-supported-egress-traffic--protocols}

{{% include-file file="docs/egress-traffic.md" shift_headings="1" %}}

---

## 2. Network Egress Contract & mTLS Tunneling {#2-network-egress-contract--mtls-tunneling}

{{% include-file file="docs/network-egress.md" shift_headings="1" %}}

---

## 3. Enabling TLS MITM Interception & Trust Bundles {#3-enabling-tls-mitm-interception--trust-bundles}

{{% include-file file="docs/egress-trust-bundle.md" shift_headings="1" %}}
