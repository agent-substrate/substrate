# DNS Controller

The DNS Controller orchestrates the configuration needed to setup the ATE routing.

We want to resolve requests for <actor-name>.<atespace>.actors.resources.substrate.ate.dev to the router service address.

* Stub resolver mode: orchestrate running a CoreDNS instance with the actor name mapped to the atenet-router service address.

Cluster resources:

* Deployment `ate-system:dns`. Label: app=dns
* Service `ate-system:dns`.

These are defined in manifests/ate-install/atenet-dns.yaml.

## Stub resolver mode

* Ensure stub resolver CoreDNS is running as:
  * Deployment `ate-system:dns`.
  * Service `ate-system:dns` pointing to the Deployment.

Corefile, rendered by `corefile.go`:

```
# Answer any 'A' query for an actor name + atespace pattern under actors.resources.substrate.ate.dev
    template IN A actors.resources.substrate.ate.dev {
        match "^[a-z0-9]([-a-z0-9]*[a-z0-9])?\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?\\.actors\\.resources\\.substrate\\.ate\\.dev\\.$"
        answer "{{ .Name }} 60 IN A <router service address>"
        fallthrough
    }
# NODATA for a well-formed actor name on any other qtype (AAAA, HTTPS, SRV, ...).
    template ANY ANY actors.resources.substrate.ate.dev {
        match "^[a-z0-9]([-a-z0-9]*[a-z0-9])?\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?\\.actors\\.resources\\.substrate\\.ate\\.dev\\.$"
        rcode NOERROR
        authority "{{ .Zone }} 60 IN SOA ns.dns.{{ .Zone }} hostmaster.{{ .Zone }} (1 60 60 60 60)"
        fallthrough
    }
# Terminal catch-all: NXDOMAIN for anything else in the zone.
    template ANY ANY actors.resources.substrate.ate.dev {
        rcode NXDOMAIN
        authority "{{ .Zone }} 60 IN SOA ns.dns.{{ .Zone }} hostmaster.{{ .Zone }} (1 60 60 60 60)"
    }
```

The last two blocks keep the zone from ever answering SERVFAIL, which musl libc
maps to `EAI_AGAIN` — sinking the paired A query with it — and which cannot be
cached negatively.

## Integration

* CoreDNS: Update CoreDNS ConfigMap to add the stub resolver.
* GKE DNS: Update the GKE DNS ConfigMap to add the stub resolver.
