# Agent Substrate

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

NOTE: This is not an officially supported Google product. This project is not
eligible for the [Google Open Source Software Vulnerability Rewards Program](https://bughunters.google.com/open-source-security).

## What is Agent Substrate?

Agent substrate is a system built on top of Kubernetes which manages agent-like
workloads to achieve higher scale and efficiency than Kubernetes alone can
offer, with lower latency.  It builds on top of Kubernetes features like
Pods and Pod autoscaling, but takes the Kubernetes control-plane out of the
critical path to achieve lower latency.

It can run on any Kubernetes cluster and does not inhibit “regular” use of
Kubernetes in any way. Kubernetes provides the infrastructure provisioning and
management for all types of workloads, while Agent Substrate provides
agent-specific scheduling and control.

At its core, Agent Substrate maps a larger set of “actors” (applications such
as agents) onto a smaller set of ready “workers” (Kubernetes Pods), relying on
the fact that agent-like applications tend to be idle most of the time to
achieve heavy multiplexing.  It provides functionality to manage an actor’s
lifecycle (e.g. create/destroy, suspend/resume), to assign actors to workers in real
time, and to route incoming traffic to them.

Agent Substrate is intended to be a low-opinion system.  The workloads it
manages don't have to be literal AI agents, but those are the best example of
the kind of applications it is designed for.  It is not an SDK for building
agents, but rather a system for running them at scale.

## Demo

[![Agent Substrate Demo](https://img.youtube.com/vi/ZEzkCFJkzjY/hq1.jpg)](https://www.youtube.com/watch?v=ZEzkCFJkzjY)

*Watch the Agent Substrate cluster multiplex ~250 stateful actor sessions across just 8 physical pods.*

This demo highlights the core developer experience and "Agentic Infrastructure" capabilities of Substrate:

1.  **Instant Session Teleport:** High-performance suspend and resume of actors onto any available worker in the pool with sub-second activation.
2.  **State Persistence:** Persistent working memory (volatile RAM) and filesystem state preserved perfectly across hibernation cycles via full-state snapshots.
3.  **Agent Swarm Multiplexing:** Demonstrates 30x+ oversubscription by "juggling" a large registry of stateful actors onto a small pool of shared physical pods.

To reproduce this demo in your own cluster, please refer to the detailed walkthroughs in the **[Counter Demo](demos/counter/README.md)** and **[Secret Agent Demo](demos/agent-secret/README.md)**.

For more videos and walkthroughs, visit our YouTube channel: **[agent-substrate](https://www.youtube.com/channel/UCN9PPqlTtVxlcpbQ-NWpfZQ)**.

## Framework Agnostic & Compatibility

Agent Substrate is designed to be **framework and agent harness agnostic**. Because it manages standard OCI containers at the kernel level (via gVisor), it can host agents built on any stack.

*   **Agent Development Kit (ADK):** Native support for ADK-compatible session identity and persistent working memory.
*   **LangChain:** Ideal execution environment for long-running, stateful LangChain agents and sandboxed tool-calling.
*   **Claude Code & CodeX:** Support for high-density, stateful coding environments that preserve terminal and filesystem state across sessions.
*   **Model Context Protocol (MCP):** Deploy secure, sandboxed MCP servers as Substrate Actors to provide durable tools for any LLM.

## Ecosystem & Examples

*   **[Agent Executor](https://github.com/google/ax):** A distributed agent runtime that demonstrates building a secure, hyper-scalable agent harness on Agent Substrate (see the [announcement blog](https://cloud.google.com/blog/products/ai-machine-learning/agent-executor-googles-distributed-agent-runtime) and [integration guide](https://github.com/google/ax/blob/main/manifests/README.md)).

## Status and compatibility

Agent Substrate is currently in VERY early development.  It is not ready for
production use, and the APIs are almost guaranteed to change.  We are not
making any guarantees about backward compatibility at this stage, and
everything in this project may be changed.

### Supported Kubernetes Releases

Currently we aim to support the [latest stable release](https://kubernetes.io/releases/) of Kubernetes, and the previous minor release.

## Community

For announcements, technical discussions, and community support, please join
the **[ate-dev](https://groups.google.com/g/ate-dev)** Google Group.

We host a weekly community meeting every Thursday from 10:00am - 11:00am PST.
- Video call link: https://meet.google.com/uhq-cxvn-dhy
- Or dial: (US) +1 253-289-6971 PIN: 787 664 574 59#
- More phone numbers: https://tel.meet/uhq-cxvn-dhy?pin=9044088223662

We also have channels in the CNCF slack; [request an invite here](https://slack.cncf.io/)
if you don't have access.

- [#substrate-users](https://cloud-native.slack.com/archives/C0B6RCAJULW) to discuss using substrate.
- [#substrate-dev](https://cloud-native.slack.com/archives/C0B6M3E2J3D) to discuss developing substrate.

## Developing

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on contributing to
the project.  We welcome contributions of all kinds, but the project is VERY
young.  Our immediate focus is on building out the core system and demos, so we
may not be able to review or merge contributions that don't align with those
goals in the near term.

## Quickstart (Development)

To quickly set up the complete environment:

1. Make sure you have [Go](https://go.dev/doc/install), [`kubectl`](https://kubernetes.io/docs/tasks/tools/), and [`docker`](https://www.docker.com/) installed and configured on your dev machine. We will automatically manage other dependencies via Go, including [`kind`](https://kind.sigs.k8s.io/).

2. Run the following steps:
```shell
# create cluster and local registry
hack/create-kind-cluster.sh

# install ate, valkey, rustfs
hack/install-ate-kind.sh --deploy-ate-system

# install counter demo
hack/install-ate-kind.sh --deploy-demo-counter

# install kubectl-ate
go install ./cmd/kubectl-ate

# create a counter actor and demo it
kubectl ate create actor my-counter-1 --template ate-demo-counter/counter

# port-forward the network router to bind to local port `8000`
kubectl port-forward -n ate-system svc/atenet-router 8000:80
```

3. In a **separate terminal**, send an HTTP request to increment the counter:
```shell
curl -X POST -H "Host: my-counter-1.actors.resources.substrate.ate.dev" -i http://localhost:8000/
```

### GKE Quickstart (Development)

1. Create and configure your environment file:
   ```bash
   cp hack/ate-dev-env.sh.example .ate-dev-env.sh

   # Edit .ate-dev-env.sh to match your project and preferences, then source it:
   source .ate-dev-env.sh
   ```

2. Enable application-default credentials for gcloud:
   ```bash
   gcloud auth application-default login --project=${PROJECT_ID}
   ```

3. Provision the required GCP resources (GKE cluster, Redis, GCS, and IAM bindings):
   ```bash
   go run ./tools/setup-gcp --all
   ```

4. Deploy the Agent Substrate system to your cluster (remember to navigate back to root directory of this repo before running the following commands):
   ```bash
   ./hack/install-ate.sh --deploy-ate-system
   ```

5. You can then deploy the sample applications. See [demos/counter/README.md](demos/counter/README.md) or [demos/sandbox/README.md](demos/sandbox/README.md) for detailed walkthroughs.
   ```bash
   ./hack/install-ate.sh --deploy-demo-counter
   ```

#### Custom Setup and Deployment

You can run individual setup steps to create GCP resources as needed. See `go run ./tools/setup-gcp --help` for available options. For example:
```bash
go run ./tools/setup-gcp --create-cluster
go run ./tools/setup-gcp --create-gvisor-node-pool
```

Similarly, you can deploy or cleanup specific Agent Substrate components using the installation script. See `./hack/install-ate.sh --help` for all options.
```bash
# Re-deploy only ate-apiserver of the ATE system
./hack/install-ate-kind.sh --deploy-ate-apiserver

# Delete everything (core system and all demos)
./hack/install-ate-kind.sh --delete-all
```

#### Tearing down resources (GCP)

If you need to delete the resources created by the setup script, you can use the provided script `hack/teardown.sh`. This script will delete resources in the reverse order of creation and handles partial failures gracefully.

```bash
./hack/teardown.sh --all
```

Or run individual teardown steps as needed (see `./hack/teardown.sh` for available options).

#### Tearing down local `kind` resources

If you need to delete the local `kind` cluster and its registry (if it was created by `hack/create-kind-cluster.sh`):

```bash
./hack/delete-kind-cluster.sh
```

## Demos

We provide several sample applications demonstrating Agent Substrate's capabilities:

1. **[Counter Demo](demos/counter/README.md)**: A stateful Go HTTP server demonstrating state preservation across suspends/resumes, and dynamic CRD routing.
2. **[Sandbox Demo (Antigravity)](demos/sandbox/README.md)**: A secure, sandboxed execution environment (running Alpine Linux) that allows arbitrary shell execution while preserving filesystem state across sessions.
3. **[Claude Code Multiplex](demos/claude-code-multiplex/README.md)**: Demonstrates oversubscribing physical hardware by multiplexing multiple Claude Code agents onto a limited pool of workers.
4. **[Secret Agent](demos/agent-secret/README.md)**: Highlights Substrate's "Zero-Idle" self-suspension and re-animation of volatile process memory.

### Documentation & Guides
* [API Configuration Guide](docs/api-guide.md): Detailed reference for configuring WorkerPools, ActorTemplates, Secrets, and Volumes.
* [Full CLI Documentation](cmd/kubectl-ate/README.md): Installation and usage for `kubectl-ate`.
* [Glossary](docs/glossary.md): Core terms (Actor, ActorTemplate, WorkerPool, Worker, ate-api-server, atenet, atelet, ateom) and how they relate.
* [Observability Guide](docs/observability.md): Guide to actor logging, metrics, and distributed tracing.

## Tour

### Commands

* `cmd/ateapi`: The core control plane API server exposing gRPC endpoints to manage actor and worker lifecycles.
* `cmd/atelet`: A node-level DaemonSet that supervises physical worker pods, coordinates snapshotting, and manages state transfers.
* `cmd/atecontroller`: A Kubernetes controller that reconciles WorkerPool and ActorTemplate custom resources.
* `cmd/atenet`: A combined networking controller providing DNS, Envoy routing, and proxy sidecars.
* `cmd/ateom-gvisor`: An interior-pod helper running inside sandboxed worker pods to execute `runsc` checkpoint and restore commands.
* `cmd/podcertcontroller`: A "polyfill" that provides Pod Certificate signers that
  will eventually ship in upstream Kubernetes (with different names).
* `cmd/kubectl-ate`: A CLI tool for managing Agent Substrate resources. See its [README](cmd/kubectl-ate/README.md).
* `tools/setup-gcp`: A provisioning utility to set up the necessary GCP infrastructure resources (GKE, GCS, IAM).
* `demos/`: Sample applications demonstrating Agent Substrate capabilities.
