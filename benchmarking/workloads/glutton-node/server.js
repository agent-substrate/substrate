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

// glutton-node serves the subset of glutton's HTTP routes that GluttonUser
// drives for its memory load: /readyz, /ping, /writeram and /readram. The
// messages are those of internal/proto/glutton/glutton.proto, encoded by hand
// so the image needs no npm packages.
//
// The working set lives on the V8 heap as arrays of doubles, so suspend and
// resume see the memory layout and garbage collector of a Node.js process.
// glutton (Go) keeps it in []byte instead. The routes glutton-node lacks,
// such as /usecpu, answer 404.

'use strict';

const http = require('node:http');

const PAGE_SIZE = 4096;
// Elements per chunk: 1 MiB of doubles. Each chunk is its own heap object,
// so a fill does not need one contiguous allocation.
const CHUNK_ELEMS = (1 << 20) / 8;
const PAGE_ELEMS = PAGE_SIZE / 8;

// WriteMode values from glutton.proto.
const WRITE_MODE_TRUNCATE = 0;
const WRITE_MODE_OVERWRITE = 1;
const WRITE_MODE_OVERWRITE_ROTATE = 2;

class HTTPError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

// parseBytes matches glutton's parseBytes: plain bytes, or a Ki/Mi/Gi
// (binary) or K/M/G (decimal) suffix.
function parseBytes(s) {
  const suffixes = [
    ['Ki', 1024], ['Mi', 1024 ** 2], ['Gi', 1024 ** 3],
    ['K', 1e3], ['M', 1e6], ['G', 1e9],
  ];
  let mult = 1;
  let num = s;
  for (const [suffix, m] of suffixes) {
    if (s.endsWith(suffix)) {
      mult = m;
      num = s.slice(0, -suffix.length);
      break;
    }
  }
  num = num.trim();
  if (!/^-?\d+$/.test(num)) {
    throw new HTTPError(400, `size: parsing size ${JSON.stringify(s)}`);
  }
  const n = Number(num) * mult;
  if (n < 0) {
    throw new HTTPError(400, 'size must be non-negative');
  }
  return n;
}

// decode reads the fields of a protobuf message into {fieldNumber: value}.
// Varints come back as Numbers and length-delimited fields as strings, which
// covers every request message served here.
function decode(buf) {
  const out = {};
  let i = 0;
  const varint = () => {
    let result = 0;
    let mult = 1;
    for (;;) {
      if (i >= buf.length) throw new HTTPError(400, 'unmarshal: truncated varint');
      const b = buf[i++];
      result += (b & 0x7f) * mult;
      if ((b & 0x80) === 0) return result;
      mult *= 128;
    }
  };
  while (i < buf.length) {
    const key = varint();
    const field = Math.floor(key / 8);
    switch (key & 7) {
      case 0:
        out[field] = varint();
        break;
      case 2: {
        const len = varint();
        if (i + len > buf.length) throw new HTTPError(400, 'unmarshal: truncated field');
        out[field] = buf.toString('utf8', i, i + len);
        i += len;
        break;
      }
      default:
        throw new HTTPError(400, `unmarshal: unsupported wire type ${key & 7}`);
    }
  }
  return out;
}

function encodeVarint(n, bytes) {
  while (n >= 128) {
    bytes.push((n % 128) | 0x80);
    n = Math.floor(n / 128);
  }
  bytes.push(n);
}

// encode writes fields given as [fieldNumber, value] pairs: strings as
// length-delimited, non-negative integers as varints. Zero values are
// omitted, as proto3 does.
function encode(fields) {
  const bytes = [];
  for (const [field, value] of fields) {
    if (typeof value === 'string') {
      if (value === '') continue;
      const b = Buffer.from(value, 'utf8');
      encodeVarint(field * 8 + 2, bytes);
      encodeVarint(b.length, bytes);
      bytes.push(...b);
    } else {
      if (value === 0) continue;
      encodeVarint(field * 8, bytes);
      encodeVarint(value, bytes);
    }
  }
  return Buffer.from(bytes);
}

// ram maps a key to {chunks, elems, cursor}: elems is the array size in
// doubles, and cursor is where the next rotate starts.
const ram = new Map();

// randomChunk builds the chunk with push, so V8 keeps it a packed double
// array rather than a holey or dictionary one.
function randomChunk(n) {
  const chunk = [];
  for (let j = 0; j < n; j++) chunk.push(Math.random());
  return chunk;
}

function newArray(elems) {
  const chunks = [];
  for (let left = elems; left > 0; left -= CHUNK_ELEMS) {
    chunks.push(randomChunk(Math.min(left, CHUNK_ELEMS)));
  }
  return { chunks, elems, cursor: 0 };
}

// randomize overwrites count elements from start, wrapping at the end.
function randomize(arr, start, count) {
  for (let k = 0; k < count; k++) {
    const e = (start + k) % arr.elems;
    arr.chunks[Math.floor(e / CHUNK_ELEMS)][e % CHUNK_ELEMS] = Math.random();
  }
}

function writeRAM(req) {
  const key = req[1] || '';
  if (key === '') throw new HTTPError(400, 'key is required');
  const elems = Math.floor(parseBytes(req[2] || '0') / 8);
  const mode = req[3] || WRITE_MODE_TRUNCATE;
  switch (mode) {
    case WRITE_MODE_TRUNCATE:
      ram.set(key, newArray(elems));
      break;
    case WRITE_MODE_OVERWRITE: {
      const arr = ram.get(key);
      if (!arr || elems > arr.elems) {
        ram.set(key, newArray(elems));
      } else {
        randomize(arr, 0, elems);
      }
      break;
    }
    case WRITE_MODE_OVERWRITE_ROTATE: {
      const arr = ram.get(key);
      if (!arr || arr.elems === 0) {
        throw new HTTPError(404, `rotate needs an existing array ${JSON.stringify(key)}; fill with TRUNCATE first`);
      }
      const count = Math.min(elems, arr.elems);
      randomize(arr, arr.cursor, count);
      arr.cursor = (arr.cursor + count) % arr.elems;
      break;
    }
    default:
      throw new HTTPError(400, `unknown write_mode ${mode}`);
  }
  return encode([]);
}

// readRAM reads one element per page, like glutton reads one byte per page,
// and XORs them into a checksum so the reads are observable.
function readRAM(req) {
  const key = req[1] || '';
  if (key === '') throw new HTTPError(400, 'key is required');
  const arr = ram.get(key);
  if (!arr) throw new HTTPError(404, `no RAM array ${JSON.stringify(key)}`);
  let walk = arr.elems;
  if (req[2]) walk = Math.min(Math.floor(parseBytes(req[2]) / 8), walk);
  let sum = 0;
  for (let e = 0; e < walk; e += PAGE_ELEMS) {
    sum = (sum ^ (arr.chunks[Math.floor(e / CHUNK_ELEMS)][e % CHUNK_ELEMS] * 0x100000000)) >>> 0;
  }
  return encode([[1, walk * 8], [2, sum]]);
}

function ping(req) {
  return encode([[1, req[1] || '']]);
}

const routes = { '/ping': ping, '/writeram': writeRAM, '/readram': readRAM };

const server = http.createServer((request, response) => {
  if (request.url === '/readyz') {
    response.writeHead(200).end();
    return;
  }
  const handler = routes[request.url];
  if (!handler) {
    response.writeHead(404).end('not found');
    return;
  }
  if (request.method !== 'POST') {
    response.writeHead(405).end('method not allowed');
    return;
  }
  const body = [];
  request.on('data', (b) => body.push(b));
  request.on('end', () => {
    try {
      const out = handler(decode(Buffer.concat(body)));
      response.writeHead(200, { 'Content-Type': 'application/x-protobuf' }).end(out);
    } catch (e) {
      response.writeHead(e.status || 500).end(e.message);
    }
  });
});

// No idle timeout, like glutton: the ingress proxy pools connections for
// longer than Node's 5s default and would hit closed ones after a suspend.
server.keepAliveTimeout = 0;

const port = Number(process.env.PORT || 80);
server.listen(port, () => console.log(`glutton-node listening on :${port}`));
