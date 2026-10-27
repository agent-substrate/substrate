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

const assert = require('node:assert/strict');
const {test} = require('node:test');
const timers = require('node:timers/promises');
const {run, checkStatus, lacksSignoff, missingSignoff, syncPull} = require('./label.cjs');

function commit(sha = 'head', message = 'change') {
  return {
    sha, parents: [{}], author: {type: 'User'},
    commit: {
      message,
      author: {name: 'Contributor', email: 'author@example.com'},
      committer: {name: 'Integrator', email: 'committer@example.com'},
    },
  };
}

function pull(conclusion, labelled = false) {
  return {
    id: 'PR_1', number: 1, state: 'OPEN', headRefOid: 'head', baseRefOid: 'base', baseRefName: 'main',
    labels: {nodes: [{name: 'kind/bug'}, ...(labelled ? [{name: 'needs-dco'}] : [])], pageInfo: {hasNextPage: false}},
    commits: {totalCount: 1, nodes: [{commit: {checkSuites: {
      pageInfo: {hasNextPage: false},
      nodes: conclusion ? [{
        app: {databaseId: 974774, slug: 'dco-2'},
        checkRuns: {pageInfo: {hasNextPage: false}, nodes: [{
          databaseId: 1, name: 'DCO', status: 'COMPLETED', conclusion,
        }]},
      }] : [],
    }}}]},
  };
}

function client(t, pr, commits = [commit()]) {
  t.mock.method(timers, 'setTimeout', async () => {});
  const writes = [];
  const logs = [];
  const github = {
    graphql: t.mock.fn(async () => ({node: structuredClone(pr)})),
    paginate: t.mock.fn(async () => commits),
    rest: {
      pulls: {listCommits: 'listCommits'},
      issues: Object.fromEntries(['getLabel', 'createLabel', 'addLabels', 'removeLabel'].map(method => [
        method, t.mock.fn(async args => { if (method !== 'getLabel') writes.push({method, args}); }),
      ])),
    },
  };
  return {
    github, writes, logs,
    context: {repo: {owner: 'org', repo: 'repo'}, eventName: 'workflow_dispatch', payload: {}},
    core: {info: text => logs.push(text)},
  };
}

test('fallback matches DCO2 sign-off identities and exemptions', () => {
  const cases = [
    ['absent', commit(), true],
    ['author', commit('head', 'change\n\nSigned-off-by: Contributor <author@example.com>'), false],
    ['committer', commit('head', 'change\n\nSigned-off-by: Integrator <committer@example.com>'), false],
    ['case insensitive', commit('head', 'change\n\nsigned-off-by: CONTRIBUTOR <AUTHOR@EXAMPLE.COM>\r\n'), false],
    ['wrong identity', commit('head', 'Signed-off-by: Someone Else <else@example.com>'), true],
    ['mixed identities', commit('head', 'Signed-off-by: Contributor <committer@example.com>'), true],
    ['multiple signoffs', commit('head', 'Signed-off-by: Other <other@example.com>\nSigned-off-by: Contributor <author@example.com>'), false],
    ['quoted example', commit('head', '> Signed-off-by: Contributor <author@example.com>'), true],
    ['merge', {...commit(), parents: [{}, {}]}, false],
    ['bot', {...commit(), author: {type: 'Bot'}}, false],
    ['unlinked author', {...commit(), author: null}, true],
    ['signed cryptographically', {...commit(), commit: {...commit().commit, verification: {verified: true}}}, true],
  ];
  for (const [name, c, expected] of cases) assert.equal(lacksSignoff(c), expected, name);
});

test('only a completed DCO2 verdict changes the label', () => {
  for (const [conclusion, expected] of [
    ['ACTION_REQUIRED', true], ['SUCCESS', false], ['FAILURE', null],
    ['NEUTRAL', null], ['SKIPPED', null], ['CANCELLED', null], [undefined, undefined],
  ]) assert.equal(checkStatus(pull(conclusion)), expected, conclusion);
  const pr = pull('ACTION_REQUIRED');
  const suite = pr.commits.nodes[0].commit.checkSuites.nodes[0];
  suite.checkRuns.nodes.push({databaseId: 2, name: 'DCO', status: 'IN_PROGRESS'});
  assert.equal(checkStatus(pr), null, 'new pending run supersedes old failure');
  suite.app = {databaseId: 15368, slug: 'github-actions'};
  assert.equal(checkStatus(pr), undefined, 'a workflow named DCO cannot pass the check');
  pr.commits.nodes[0].commit.checkSuites.pageInfo.hasNextPage = true;
  assert.equal(checkStatus(pr), null, 'incomplete checks cannot supply a verdict');
});

test('legacy fork PR with unsigned commits and a merge gets labelled', async t => {
  const pr = pull();
  pr.commits.totalCount = 3;
  const api = client(t, pr, [commit('one'), commit('two'), {...commit(), parents: [{}, {}]}]);
  await syncPull(api, pr, false);
  assert.deepEqual(api.writes, [{method: 'addLabels', args: {
    owner: 'org', repo: 'repo', issue_number: 1, labels: ['needs-dco'],
  }}]);
});

test('app success including an override takes precedence over missing trailers', async t => {
  const pr = pull('SUCCESS', true);
  const api = client(t, pr);
  await syncPull(api, pr, false);
  assert.equal(api.github.paginate.mock.callCount(), 0);
  assert.equal(api.writes[0].method, 'removeLabel');
  assert.equal(api.writes[0].args.name, 'needs-dco');
});

test('pending or failed evaluation leaves an existing label untouched', async t => {
  const pr = pull('FAILURE', true);
  const api = client(t, pr);
  await syncPull(api, pr, false);
  assert.equal(api.github.paginate.mock.callCount(), 0);
  assert.deepEqual(api.writes, []);
});

test('fallback reads every page and catches an unsigned commit before a signed head', async t => {
  const pr = pull();
  pr.commits.totalCount = 101;
  const commits = Array.from({length: 100}, (_, i) => commit(String(i), 'Signed-off-by: Contributor <author@example.com>'));
  commits[99] = commit('99');
  commits.push(commit('head', 'Signed-off-by: Contributor <author@example.com>'));
  const api = client(t, pr, commits);
  assert.equal(await missingSignoff(api.github, api.context.repo, pr), true);
  assert.deepEqual(api.github.paginate.mock.calls[0].arguments, ['listCommits', {
    owner: 'org', repo: 'repo', pull_number: 1, per_page: 100,
  }]);
});

test('incomplete, duplicate, empty, oversized, or changed commit lists stay undetermined', async t => {
  for (const [count, commits] of [
    [2, [commit()]], [2, [commit(), commit()]], [0, []],
    [251, [commit()]], [1, [commit('old-head')]],
  ]) {
    const pr = pull(undefined, true);
    pr.commits.totalCount = count;
    const api = client(t, pr, commits);
    await syncPull(api, pr, false);
    assert.deepEqual(api.writes, []);
  }
});

test('signing off the new head clears a legacy label without an app run', async t => {
  const pr = pull(undefined, true);
  const api = client(t, pr, [commit('head', 'Signed-off-by: Contributor <author@example.com>')]);
  await syncPull(api, pr, false);
  assert.equal(api.writes[0].method, 'removeLabel');
});

test('head, base SHA, base branch, or state changing prevents stale writes', async t => {
  for (const change of [
    {headRefOid: 'new'}, {baseRefOid: 'new'}, {baseRefName: 'release-0.6'}, {state: 'CLOSED'},
  ]) {
    const pr = pull('ACTION_REQUIRED');
    const api = client(t, {...pr, ...change});
    await syncPull(api, pr, false);
    assert.deepEqual(api.writes, []);
  }
});

test('app verdict arriving during fallback is authoritative', async t => {
  const pr = pull();
  const api = client(t, pull('SUCCESS'));
  await syncPull(api, pr, false);
  assert.deepEqual(api.writes, []);
});

test('dry run reads all PR pages and neither creates nor changes labels', async t => {
  const pr = pull('ACTION_REQUIRED');
  const api = client(t, pr);
  api.github.graphql.mock.mockImplementation(async (_query, vars) => {
    if (vars.id) return {node: pr};
    return {repository: {pullRequests: {
      nodes: vars.cursor ? [] : [pr],
      pageInfo: {hasNextPage: !vars.cursor, endCursor: 'next'},
    }}};
  });
  await run(api);
  assert.deepEqual(api.writes, []);
  assert.equal(api.github.rest.issues.getLabel.mock.callCount(), 0);
  assert.ok(api.logs.includes('#1: would add needs-dco'));
  assert.equal(api.github.graphql.mock.calls.at(-1).arguments[1].cursor, 'next');
});

test('PR pushes and retargets update labels; title and body edits skip API calls', async t => {
  for (const [action, changes, expected] of [
    ['synchronize', undefined, true],
    ['edited', {base: {ref: {from: 'main'}, sha: {from: 'base'}}}, true],
    ['edited', {title: {from: 'old title'}}, false],
    ['edited', {body: {from: 'old body'}}, false],
  ]) {
    const pr = pull('ACTION_REQUIRED');
    const api = client(t, pr);
    api.context.eventName = 'pull_request_target';
    api.context.payload = {action, changes, pull_request: {node_id: pr.id}};
    api.github.rest.issues.getLabel.mock.mockImplementation(async () => { throw {status: 404}; });
    await run(api, false);
    assert.deepEqual(api.writes.map(write => write.method), expected ? ['createLabel', 'addLabels'] : []);
    assert.equal(api.github.rest.issues.getLabel.mock.callCount(), expected ? 1 : 0);
    assert.deepEqual(api.github.graphql.mock.calls[0]?.arguments[1], expected ? {id: pr.id} : undefined);
    assert.equal(api.github.paginate.mock.callCount(), 0);
  }
});

test('API errors fail the run instead of clearing labels', async t => {
  const pr = pull(undefined, true);
  const api = client(t, pr);
  api.github.paginate.mock.mockImplementation(async () => { throw new Error('rate limited'); });
  await assert.rejects(syncPull(api, pr, false), /rate limited/);
  assert.deepEqual(api.writes, []);
});

test('label removal tolerates only a missing label', async t => {
  const pr = pull('SUCCESS', true);
  const api = client(t, pr);
  api.github.rest.issues.removeLabel.mock.mockImplementation(async () => { throw {status: 404}; });
  await syncPull(api, pr, false);
  api.github.rest.issues.removeLabel.mock.mockImplementation(async () => { throw new Error('forbidden'); });
  await assert.rejects(syncPull(api, pr, false), /forbidden/);
});
