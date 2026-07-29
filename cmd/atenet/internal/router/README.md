# router

Router has several responsibilities:

* (Optional) manages a Deployment of Envoy to function as a router for ATE requests.
  * This is optional to enable testing the router component in a standalone mode without managing the Kubernetes objects.
  * Envoy will be configured to send traffic to via xDS served by the Router.
* ext_proc server for the Envoy. To make the deployment and debugging easier, we will run this component together
  with the router, but this will be split later into its own component.
  * ext_proc will call into the ATE gRPC API to get the set of relevant backends (specific the worker IP) and
    route the traffic accordingly
  * Make sure the interface with ATE API is pluggable so that we can test with a mock ATE API.
* Runs an xDS server for the Envoy deployment that defines the Cluster information for the ATEs.
  * the xDS configuration will configure Envoy to send traffic to ext_proc
* Watches the ActorTemplates to get out the definitions for how to route the session IDs.
* Parks requests whose actor cannot be served immediately due to transient
  worker-pool saturation, retrying the resume until the actor is routable or a
  bounded wait elapses, instead of failing fast. See
  [docs/request-parking.md](../../../../../docs/request-parking.md).

## status page

Serve a `/statusz` page on port 8080.

Contents:

* Global flags values
* Command line args
* Last 100 queries served
* Build tag