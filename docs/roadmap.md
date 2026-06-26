# Agent Substrate Roadmap

## Overview

Agent Substrate is a Kubernetes-based system for building multitenant applications based on independent, stateful container instances. Substrate builds on sandboxed, snapshottable Pods by adding the ability to update running containers in the Pod without invoking the Kubernetes scheduler. Substrate enables these containers to rapidly cycle between active and inactive lifecycle phases with fast suspend and resume from snapshots in inexpensive cloud object storage, without losing in-memory or filesystem state. This is especially critical for agentic workloads, which frequently oscillate between active (handling user input) and idle (waiting for LLMs or tool calls to complete). With Substrate, resources can be reclaimed during these idle moments for use by other workloads. This enables both greater efficiency and greater scale than using Pods alone, while still leveraging Kubernetes to provision capacity, configure networking and storage, and manage Substrate itself.

This project is trying to move very quickly to find the right set of capabilities for the ever-changing agentic workloads market. Below are our priorities. Any efforts which are not aligned with these priorities should probably be deferred.

1. Pinning down architectural decisions which influence the rest of these priorities.
2. Performance and reliability of wakeups, including suspend/resume and snapshot management.
3. A fast, scalable control-plane, including the storage layer, reliability, and security.
4. Useful identity, such that policies can be written in terms of identity.
5. Useful policy, including how users can specify ingress, egress, and peer networking.
6. Runtime modularity, such that different kinds of sandboxes (gVisor, microVMs, etc) can be used.

Below is a collection of finer-grained efforts which we believe align with the above.

## Coming Soon: High Priority

### Actor Management (Compute)

* Actor versioning via ActorTemplate (or new ActorDeployment API), with support for A/B testing and rollout strategies.
* Namespaces or a similar concept for grouping related actors together for both management and convenience of writing actor-to-actor authorization policy.
* Actor Forking/Cloning: Ability to branch a new logical actor from an existing checkpoint (the 'State Root') to support complex agent reasoning paths.
* Worker horizontal autoscaling: Ability to rapidly scale up nodes and warm Pods to meet actor demand.
* Clarify the actor lifecycle, including what data is retained across which events (e.g. gvisor upgrade \-\> loss of memory snapshot)
* Decide: What mode(s) of actor runs do we support?  Can we prioritize and phase this?  Assuming we always persist “working” data
  * Clean start from OCI for every activation
  * Resume from golden for every activation
  * Persist rootfs (when possible), clean binary start for every activation
  * Persist rootfs and memory (when possible), full process resume

### Worker management

* Worker class (e.g. free-tier on cheap hardware, premium on good hardware)
* Fungible worker pools (any worker in a class can run any actor in that class)

### Control-plane

* User authorization
* Good CLI

### Networking

* Actor security boundary implementation, default deny with explicit ACLs at scale with low latency. This overlaps with some of the security items (see below).
* Policy definition: between framework (outside) and Actors, between Actors, Actor Egress.
* Standardized DNS Mesh: Moving to a production-grade routing format (\<id\>.actors.resources.substrate.ate.dev) for location-transparent actor-to-actor communication.

### Storage

* Decide: Is Redis/ValKey the right answer for API storage?
* gVisor snapshot/resume optimizations
  * storage tiering (local zswap, local SSD, peer-to-peer, blob)
  * incremental snapshots
* Support for S3 (via plugin)
* Distinct lifecycle for “rootfs” and memory snapshots vs. “working” space.  Needs API surface of where to mount.
* ConfigMaps as volumes
* Data locality in scheduling (needs to expose per-node available snapshots via API)

### Security

* Goal of two security boundaries between mutually untrusted actors that share the same Kubernetes node.
* Secure mTLS authentication and authorization between all system components.
* Secure actor-to-actor authentication and authorization policy that can be deployed in-band with actor lifecycle, maintaining low latency deployment.
* Ability to specify network policy for individual actors, including L7 protocol-aware filtering rules, that can be deployed in-band with actor lifecycle, maintaining low latency deployment.
* Credential injection, including actor identity, via proxies to eliminate exposure of cryptographic keys and bearer tokens to actors.
* Audit logging on API and lifecycle operations.
* Sandbox integrations for threat detection telemetry.
* Harden actor networking to further isolate from the surrounding node (e.g. with current networking

### Observability

* Session-Aware Telemetry Correlation: Automated OTLP export where all logs, metrics, and traces are natively tagged with the Substrate Actor ID and worker ID.
* Prometheus metrics

### Performance and Reliability

* Representative workloads/traffic patterns
* Provisioning load test and benchmarking compute/infrastructure
* Storage and visualization for benchmark results
* Integrate debugging into load tests
* State Store Scale: Horizontal sharding support (via Redis Hash Tags) to enable management of 1M+ concurrent actors.
* Disk-Only Resume Policy: Support for cost-optimized hibernation where only the filesystem state is preserved, skipping the RAM restore for stateless or "cold" start-capable agents.

### Testing

* Support for local development and testing on KinD clusters.
* Define sufficient test matrices (what container system, clouds, other configuration knobs should be tested)
* Burn-in testing to detect memory/other resource leaks
* Mock LLMs/other dependencies

### Integrations

* Tight integration with Agent Executor for deploying AX on Kubernetes.
* Agent Development Kit (ADK) Native Support: Developing first-class bindings for ADK, allowing developers to build stateful agents that natively leverage Substrate’s lifecycle management and persistent working memory.
* LangChain Remote Execution Provider: A dedicated provider for LangChain to run complex, long-running agent tools in durable, sandboxed environments.
* Native MCP Server Hosting: Built-in support for deploying Model Context Protocol (MCP) servers as managed Substrate Actors, creating a secure tool ecosystem for any LLM.
* Actor-to-Actor (A2A) Calling Model: Standardized protocol for actors to discover and call other actors within the mesh via the gateway.
* Native MCP Tool Hosting: Ability to define and deploy standard Model Context Protocol (MCP) servers as managed Substrate Actors, providing a plug-and-play ecosystem for agentic tools.

### Operability

* Control-plane upgrades
* Worker upgrades
* Gvisor upgrades
* API schema changes
* Plan for growth from small to large (esp. wrt CP sharding)

## Additional ideas we are thinking about

### Actor Management (Compute)

* Vertical worker autoscaling and IPPU (e.g. retain the memory/CPU config of the previous snapshot and update the worker pod when we assign an actor)
* Actor-\>worker selectors, taints, etc.
* Automated Garbage Collection: Background cleanup of idle actors based on configurable TTL

### Storage

* Policies to drive retention of old snapshots and controller(s) to do automated cleanups
* Peer-to-peer OCI and snapshot sharing
* K8s “projected” volumes, other than ConfigMap
* Storage model supported by external storage plugins (via CSI drivers?).
* Shared writeable storage across actors (e.g. an NFS volume).

### Security

* Ability for actors to delegate downscoped rights to children and peers.
* Image-pull credentials

### Performance and Reliability

* Shared image cache

### Integrations

* Stateful Coding Adapters: Standardized environment templates for Claude Code and CodeX to enable AI coding tasks that maintain persistent filesystem state across terminal sessions.

### Administrative

* Ability to run multiple substrates in a single cluster
