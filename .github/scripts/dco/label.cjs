// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

const timers = require('node:timers/promises');
const LABEL = 'needs-dco';

// Batch the app lookups: a REST check request for every PR consumes most of
// GITHUB_TOKEN's hourly quota before inspecting commits or changing labels.
const FIELDS = `fragment Pull on PullRequest {
  id number state headRefOid baseRefOid baseRefName
  labels(first: 100) { nodes { name } pageInfo { hasNextPage } }
  commits(last: 1) {
    totalCount
    nodes { commit { checkSuites(first: 100, filterBy: {appId: 974774}) {
      pageInfo { hasNextPage }
      nodes {
        app { databaseId slug }
        checkRuns(first: 100, filterBy: {checkName: "DCO"}) {
          pageInfo { hasNextPage }
          nodes { databaseId name status conclusion }
        }
      }
    } } }
  }
}`;
const ONE = `${FIELDS} query($id: ID!) { node(id: $id) { ...Pull } }`;
const OPEN = `${FIELDS} query($owner: String!, $repo: String!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    pullRequests(first: 25, after: $cursor, states: OPEN, orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo { hasNextPage endCursor }
      nodes { ...Pull }
    }
  }
}`;

// undefined means no app run; null means the app has no actionable verdict.
function checkStatus(pr) {
  const suites = pr.commits.nodes[0]?.commit.checkSuites;
  if (!suites || suites.pageInfo.hasNextPage) return null;
  const runs = [];
  for (const suite of suites.nodes) {
    if (suite.app?.databaseId !== 974774 || suite.app.slug !== 'dco-2') continue;
    if (suite.checkRuns.pageInfo.hasNextPage) return null;
    runs.push(...suite.checkRuns.nodes.filter(run => run.name === 'DCO'));
  }
  runs.sort((a, b) => a.databaseId - b.databaseId);
  const latest = runs.at(-1);
  if (!latest) return undefined;
  if (latest.status !== 'COMPLETED') return null;
  if (latest.conclusion === 'ACTION_REQUIRED') return true;
  if (latest.conclusion === 'SUCCESS') return false;
  // An app failure can mean it could not evaluate the PR, not a missing DCO.
  return null;
}

function lacksSignoff(commit) {
  if (commit.parents.length > 1 || commit.author?.type === 'Bot') return false;
  const identities = [commit.commit.author, commit.commit.committer];
  const signoffs = [...commit.commit.message.matchAll(/^Signed-off-by: (.*) <(.*)>\s*$/gim)];
  return !signoffs.some(([, name, email]) => identities.some(identity =>
    identity && name.toLowerCase() === identity.name.toLowerCase() &&
    email.toLowerCase() === identity.email.toLowerCase()));
}

async function missingSignoff(github, repo, pr) {
  // GitHub caps this REST endpoint at 250 commits. Never clear a label from
  // a partial list; a later DCO2 verdict can resolve larger PRs.
  if (pr.commits.totalCount === 0 || pr.commits.totalCount > 250) return null;
  const commits = await github.paginate(github.rest.pulls.listCommits, {
    ...repo, pull_number: pr.number, per_page: 100,
  });
  if (new Set(commits.map(commit => commit.sha)).size !== pr.commits.totalCount ||
      commits.at(-1)?.sha !== pr.headRefOid) return null;
  return commits.some(lacksSignoff);
}

function hasLabel(pr) {
  return pr.labels.nodes.some(label => label.name === LABEL);
}

async function syncPull({github, context, core}, pr, dryRun) {
  if (pr.state !== 'OPEN' || pr.labels.pageInfo.hasNextPage) return;
  const appStatus = checkStatus(pr);
  const missing = appStatus === undefined ? await missingSignoff(github, context.repo, pr) : appStatus;
  if (missing === null) {
    core.info(`#${pr.number}: undetermined; left unchanged`);
    return;
  }
  if (missing === hasLabel(pr)) return;

  // A push, retarget, close, or app override may have happened during the scan.
  const {node: current} = await github.graphql(ONE, {id: pr.id});
  if (!current || current.state !== 'OPEN' || current.headRefOid !== pr.headRefOid ||
      current.baseRefOid !== pr.baseRefOid || current.baseRefName !== pr.baseRefName ||
      current.labels.pageInfo.hasNextPage) {
    core.info(`#${pr.number}: changed during scan; left unchanged`);
    return;
  }
  const currentStatus = checkStatus(current);
  const desired = currentStatus === undefined && appStatus === undefined ? missing : currentStatus;
  if (desired == null || desired === hasLabel(current)) return;
  core.info(`#${pr.number}: ${dryRun ? 'would ' : ''}${desired ? 'add' : 'remove'} ${LABEL}`);
  if (dryRun) return;
  if (desired) {
    await github.rest.issues.addLabels({...context.repo, issue_number: pr.number, labels: [LABEL]});
  } else {
    try {
      await github.rest.issues.removeLabel({...context.repo, issue_number: pr.number, name: LABEL});
    } catch (error) {
      if (error.status !== 404) throw error;
    }
  }
  // Pace the initial backlog's writes below GitHub's secondary rate limit.
  await timers.setTimeout(1000);
}

async function run(api, dryRun = true) {
  const {github, context, core} = api;
  if (context.eventName === 'pull_request_target' && context.payload.action === 'edited' &&
      !context.payload.changes?.base) return;
  if (!dryRun) {
    try {
      await github.rest.issues.getLabel({...context.repo, name: LABEL});
    } catch (error) {
      if (error.status !== 404) throw error;
      await github.rest.issues.createLabel({
        ...context.repo, name: LABEL, color: 'e11d21',
        description: 'Commits need a Developer Certificate of Origin sign-off',
      });
    }
  }
  if (context.eventName === 'pull_request_target') {
    const {node} = await github.graphql(ONE, {id: context.payload.pull_request.node_id});
    await syncPull(api, node, dryRun);
    return;
  }
  let cursor = null;
  let checked = 0;
  do {
    const result = await github.graphql(OPEN, {...context.repo, cursor});
    const page = result.repository.pullRequests;
    for (const pr of page.nodes) {
      await syncPull(api, pr, dryRun);
      checked++;
    }
    cursor = page.pageInfo.hasNextPage ? page.pageInfo.endCursor : null;
  } while (cursor);
  core.info(`${checked} open PRs checked${dryRun ? ' (dry run)' : ''}`);
}

module.exports = {run, checkStatus, lacksSignoff, missingSignoff, syncPull};
