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

import dns from 'node:dns';
import http from 'node:http';
import fs from 'node:fs';

const req = JSON.parse(process.argv[2] || '{}');
const runtime = req.runtime || 'node-lookup';
const op = req.op || 'resolve';
const host = req.host || '';
const url = req.url || '';
const attempts = req.attempts > 0 ? req.attempts : 1;
const intervalMs = req.intervalMs > 0 ? req.intervalMs : 0;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function readNameserversFromResolvConf() {
  try {
    const content = fs.readFileSync('/etc/resolv.conf', 'utf8');
    const servers = [];
    for (const line of content.split('\n')) {
      const trimmed = line.trim();
      if (trimmed.startsWith('nameserver ')) {
        const ip = trimmed.slice('nameserver '.length).trim();
        if (ip) servers.push(ip);
      }
    }
    return servers;
  } catch {
    return [];
  }
}

function httpGetWithLookup(targetUrl, lookupFn) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(targetUrl);
    const options = {
      hostname: parsed.hostname,
      port: parsed.port || 80,
      path: parsed.pathname + parsed.search,
      method: 'GET',
      timeout: 8000,
    };
    if (lookupFn) {
      options.lookup = lookupFn;
    }
    const clientReq = http.request(options, (res) => {
      res.resume();
      res.on('end', () => resolve(res.statusCode));
    });
    clientReq.on('error', reject);
    clientReq.on('timeout', () => {
      clientReq.destroy(new Error('HTTP request timeout'));
    });
    clientReq.end();
  });
}

async function runAttempt(resolver) {
  const start = Date.now();
  try {
    if (op === 'http_get') {
      if (runtime === 'node-resolve') {
        const customLookup = (hostname, opts, cb) => {
          if (typeof opts === 'function') {
            cb = opts;
            opts = {};
          }
          resolver.resolve4(hostname, (err, addresses) => {
            if (err) return cb(err);
            if (opts && opts.all) {
              return cb(
                null,
                addresses.map((address) => ({ address, family: 4 })),
              );
            }
            cb(null, addresses[0], 4);
          });
        };
        const statusCode = await httpGetWithLookup(url, customLookup);
        return {
          success: statusCode >= 200 && statusCode < 400,
          statusCode,
          durationMs: Date.now() - start,
        };
      } else {
        const statusCode = await httpGetWithLookup(url, null);
        return {
          success: statusCode >= 200 && statusCode < 400,
          statusCode,
          durationMs: Date.now() - start,
        };
      }
    }

    if (runtime === 'node-resolve') {
      const addresses = await new Promise((resolve, reject) => {
        resolver.resolve4(host, (err, addrs) => {
          if (err) reject(err);
          else resolve(addrs);
        });
      });
      return {
        success: true,
        addresses,
        durationMs: Date.now() - start,
      };
    } else {
      const res = await dns.promises.lookup(host, { family: 4, all: true });
      return {
        success: true,
        addresses: res.map((r) => r.address),
        durationMs: Date.now() - start,
      };
    }
  } catch (err) {
    return {
      success: false,
      errorCode: err.code || err.name || 'Error',
      error: `${err.name || 'Error'}: ${err.message}${err.code ? ` (code: '${err.code}')` : ''}`,
      durationMs: Date.now() - start,
    };
  }
}

const resolver = new dns.Resolver({ timeout: 1500, tries: 2 });
const ns = readNameserversFromResolvConf();
if (ns.length > 0) {
  resolver.setServers(ns);
}

const results = [];
for (let i = 0; i < attempts; i++) {
  if (i > 0 && intervalMs > 0) {
    await sleep(intervalMs);
  }
  results.push(await runAttempt(resolver));
}

process.stdout.write(JSON.stringify({ attempts: results }) + '\n');
