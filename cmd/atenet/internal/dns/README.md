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

`corefile.go` renders the zone below; the controller writes it to
`--corefile-path` on an emptyDir shared with the CoreDNS container and signals
a reload. The excerpt is illustrative — `corefile.go` is authoritative, and
`TestMakeCoreFile` pins the exact rendering for each family combination.

```
# Answer any 'A' query for an actor name + atespace pattern under actors.resources.substrate.ate.dev
    template IN A actors.resources.substrate.ate.dev {
        match "^[a-z0-9]([-a-z0-9]*[a-z0-9])?\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?\\.actors\\.resources\\.substrate\\.ate\\.dev\\.$"
        answer "{{ .Name }} 60 IN A <router service IPv4 ClusterIP>"
        fallthrough
    }
# The same for 'AAAA', when the router Service has an IPv6 ClusterIP.
    template IN AAAA actors.resources.substrate.ate.dev {
        match "^[a-z0-9]([-a-z0-9]*[a-z0-9])?\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?\\.actors\\.resources\\.substrate\\.ate\\.dev\\.$"
        answer "{{ .Name }} 60 IN AAAA <router service IPv6 ClusterIP>"
        fallthrough
    }
# NODATA for a well-formed actor name on any other qtype (HTTPS, SRV, ...), and
# for the family the router has no ClusterIP in.
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

An address block is emitted only for a family the atenet-router Service
actually has a ClusterIP in, which on any cluster where `ipFamilyPolicy` is
unset means exactly one of the two. That is not tidiness: the `answer` line is a
literal RR, so an `IN A` carrying an IPv6 address parses fine as a Corefile and
then fails `dns.NewRR` on every query, SERVFAILing the whole zone. Leaving the
family out hands it to the NODATA block instead, which is the right answer for a
name with no address of that type.

The last two blocks keep the zone from ever answering SERVFAIL, which musl libc
maps to `EAI_AGAIN` — sinking the paired A query with it — and which cannot be
cached negatively. The `fallthrough` on every block that carries a `match` is
load-bearing: the template plugin walks past a class or qtype mismatch on its
own, but a regex miss returns SERVFAIL immediately unless the block declares it.

## Integration

* CoreDNS: Update CoreDNS ConfigMap to add the stub resolver.
* GKE DNS: Update the GKE DNS ConfigMap to add the stub resolver.
