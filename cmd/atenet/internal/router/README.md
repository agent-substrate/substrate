# router

Router has several responsibilities:

* Runs the actor-aware routing core: resolve actor identity from a request,
  resume/locate the actor via the ATE gRPC API, and pick a worker endpoint.
  * Make sure the interface with ATE API is pluggable so that we can test with a mock ATE API.
* Exposes that routing core over ext_proc, so any proxy that supports the
  ExtProc protocol (and dynamic forwarding) can call it. Substrate does not
  ship or manage a proxy — see [`docs/dev/ingress.md`](../../../../docs/dev/ingress.md)
  for reference gateway configs.
* Watches the ActorTemplates to get out the definitions for how to route the session IDs.

## status page

Serve a `/statusz` page on the router's status port.

Contents:

* Global flags values
* Command line args
* Last 100 queries served
* Build tag