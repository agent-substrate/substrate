---
title: "Agent Substrate"
linkTitle: "Agent Substrate"
description: >
  A secure-by-default execution runtime that runs many more agents on far fewer
  Kubernetes Pods.
---

{{< blocks/cover title="Agent Substrate" subtitle="Run millions of agents on Kubernetes" height="full" >}}
<p class="cover-tagline">
  A secure-by-default agent execution runtime with sub-second suspend and
  resume, gVisor and microVM isolation, and heavy multiplexing of idle actors
  onto ready workers.
</p>
<div class="cover-actions">
  <a class="btn btn-lg btn-primary" href="docs/overview/">Get Started <i class="fas fa-arrow-right ms-2"></i></a>
  <a class="btn btn-lg btn-outline-light" href="https://github.com/agent-substrate/substrate" target="_blank" rel="noopener"><i class="fab fa-github me-2"></i>GitHub</a>
</div>

<div class="info-container">
  <div class="info-card">
    <p class="title">Actor Teleport</p>
    <p class="description">Suspend an actor and resume it on any available worker in the pool, with sub-second activation.</p>
  </div>
  <div class="info-card">
    <p class="title">State Persistence</p>
    <p class="description">Memory and filesystem state survive hibernation through full-state snapshots, so actors pick up where they left off.</p>
  </div>
  <div class="info-card">
    <p class="title">Agent Multiplexing</p>
    <p class="description">Agents are idle most of the time. Substrate packs a large registry of stateful actors onto a small pool of shared Pods.</p>
  </div>
</div>
{{< /blocks/cover >}}

{{% blocks/section color="dark" type="row" %}}

{{% blocks/feature icon="fab fa-github" title="Contributions welcome" url="https://github.com/agent-substrate/substrate" %}}
We use a [pull request](https://github.com/agent-substrate/substrate/pulls)
workflow on GitHub. Start with the
[contributing guide](https://github.com/agent-substrate/substrate/blob/main/CONTRIBUTING.md).
{{% /blocks/feature %}}

{{% blocks/feature icon="fab fa-slack" title="Chat with us" url="https://slack.cncf.io/" %}}
Join [#substrate-users](https://cloud-native.slack.com/archives/C0B6RCAJULW)
and [#substrate-dev](https://cloud-native.slack.com/archives/C0B6M3E2J3D) on
the CNCF Slack.
{{% /blocks/feature %}}

{{% blocks/feature icon="fa fa-envelope" title="Mailing list" url="https://groups.google.com/g/ate-dev" %}}
Announcements and technical discussion happen on
[ate-dev](https://groups.google.com/g/ate-dev).
{{% /blocks/feature %}}

{{% /blocks/section %}}
