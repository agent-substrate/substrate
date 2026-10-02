// Copyright 2026 Google LLC
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

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const { termPattern, sortTerms, findMatch, pickMention } = require(
  path.join(__dirname, '..', '..', 'assets', 'js', 'glossary.js'),
);

test('exact-casing pattern is case-sensitive', () => {
  assert.ok(termPattern('Worker', false).test('a Worker pod'));
  assert.ok(!termPattern('Worker', false).test('a worker thread'));
});

test('ignore-case pattern matches any casing', () => {
  assert.ok(termPattern('Worker', true).test('a worker thread'));
  assert.ok(termPattern('Resume', true).test('RESUME'));
});

test('a plural s matches', () => {
  assert.ok(termPattern('Actor', false).test('two Actors run'));
});

test('letters, digits and hyphens bound a term', () => {
  assert.ok(!termPattern('Actor', false).test('an ActorTemplate'));
  assert.ok(!termPattern('ate', true).test('run ate-api-server'));
  assert.ok(!termPattern('atelet', false).test('see atelets-x'));
  assert.ok(termPattern('ate', false).test('the ate prefix'));
  assert.ok(termPattern('Golden Snapshot', false).test('the Golden Snapshot is'));
});

test('underscores bound a term', () => {
  assert.ok(!termPattern('Actor', true).test('ACTOR_STATE_CRASHED'));
  assert.ok(!termPattern('Worker', true).test('the worker_id field'));
});

test('a whole-only text matches only when the term is all of it', () => {
  // Inline code: a card belongs on `atelet`, not inside ate.workerpool.workers.
  assert.equal(pickMention(['ate.workerpool.workers'], 'WorkerPool', [true]), null);
  assert.deepEqual(pickMention([' WorkerPool '], 'WorkerPool', [true]), { textIndex: 0, index: 1, length: 10 });
  assert.deepEqual(pickMention(['ate.workerpool.workers', 'a WorkerPool here'], 'WorkerPool', [true, false]), {
    textIndex: 1,
    index: 2,
    length: 10,
  });
});

test('longer terms sort first without mutating the input', () => {
  const terms = [{ term: 'Actor' }, { term: 'ActorTemplate' }, { term: 'ate' }];
  assert.deepEqual(sortTerms(terms).map((t) => t.term), ['ActorTemplate', 'Actor', 'ate']);
  assert.equal(terms[0].term, 'Actor');
});

test('findMatch returns the first occurrence', () => {
  assert.deepEqual(findMatch('Actor and Actor', termPattern('Actor', false)), { index: 0, length: 5 });
  assert.equal(findMatch('nothing here', termPattern('Actor', false)), null);
});

test('pickMention prefers the exact casing anywhere on the page', () => {
  assert.deepEqual(pickMention(['resume the upload', 'Resume an Actor'], 'Resume'), {
    textIndex: 1,
    index: 0,
    length: 6,
  });
});

test('pickMention falls back to any casing', () => {
  assert.deepEqual(pickMention(['so resume the upload'], 'Resume'), { textIndex: 0, index: 3, length: 6 });
  assert.equal(pickMention(['no mention'], 'Resume'), null);
});
