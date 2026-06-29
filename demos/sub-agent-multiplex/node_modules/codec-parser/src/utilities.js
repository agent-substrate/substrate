/* Copyright 2020-2023 Ethan Halsall
    
    This file is part of codec-parser.
    
    codec-parser is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    codec-parser is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>
*/

import { length, uint8Array } from "./constants.js";

const getCrcTable = (crcTable, crcInitialValueFunction, crcFunction) => {
  for (let byte = 0; byte < crcTable[length]; byte++) {
    let crc = crcInitialValueFunction(byte);

    for (let bit = 8; bit > 0; bit--) crc = crcFunction(crc);

    crcTable[byte] = crc;
  }
  return crcTable;
};

const crc8Table = getCrcTable(
  new uint8Array(256),
  (b) => b,
  (crc) => (crc & 0x80 ? 0x07 ^ (crc << 1) : crc << 1),
);

const flacCrc16Table = [
  getCrcTable(
    new Uint16Array(256),
    (b) => b << 8,
    (crc) => (crc << 1) ^ (crc & (1 << 15) ? 0x8005 : 0),
  ),
];

const crc32Table = [
  getCrcTable(
    new Uint32Array(256),
    (b) => b,
    (crc) => (crc >>> 1) ^ ((crc & 1) * 0xedb88320),
  ),
];

// build crc tables
for (let i = 0; i < 15; i++) {
  flacCrc16Table.push(new Uint16Array(256));
  crc32Table.push(new Uint32Array(256));

  for (let j = 0; j <= 0xff; j++) {
    flacCrc16Table[i + 1][j] =
      flacCrc16Table[0][flacCrc16Table[i][j] >>> 8] ^
      (flacCrc16Table[i][j] << 8);

    crc32Table[i + 1][j] =
      (crc32Table[i][j] >>> 8) ^ crc32Table[0][crc32Table[i][j] & 0xff];
  }
}

const crc8 = (data) => {
  let crc = 0;
  const dataLength = data[length];

  for (let i = 0; i !== dataLength; i++) crc = crc8Table[crc ^ data[i]];

  return crc;
};

const flacCrc16 = (data) => {
  const dataLength = data[length];
  const crcChunkSize = dataLength - 16;
  let crc = 0;
  let i = 0;

  while (i <= crcChunkSize) {
    crc ^= (data[i++] << 8) | data[i++];
    crc =
      flacCrc16Table[15][crc >> 8] ^
      flacCrc16Table[14][crc & 0xff] ^
      flacCrc16Table[13][data[i++]] ^
      flacCrc16Table[12][data[i++]] ^
      flacCrc16Table[11][data[i++]] ^
      flacCrc16Table[10][data[i++]] ^
      flacCrc16Table[9][data[i++]] ^
      flacCrc16Table[8][data[i++]] ^
      flacCrc16Table[7][data[i++]] ^
      flacCrc16Table[6][data[i++]] ^
      flacCrc16Table[5][data[i++]] ^
      flacCrc16Table[4][data[i++]] ^
      flacCrc16Table[3][data[i++]] ^
      flacCrc16Table[2][data[i++]] ^
      flacCrc16Table[1][data[i++]] ^
      flacCrc16Table[0][data[i++]];
  }

  while (i !== dataLength)
    crc = ((crc & 0xff) << 8) ^ flacCrc16Table[0][(crc >> 8) ^ data[i++]];

  return crc;
};

const crc32Function = (data) => {
  const dataLength = data[length];
  const crcChunkSize = dataLength - 16;
  let crc = 0;
  let i = 0;

  while (i <= crcChunkSize)
    crc =
      crc32Table[15][(data[i++] ^ crc) & 0xff] ^
      crc32Table[14][(data[i++] ^ (crc >>> 8)) & 0xff] ^
      crc32Table[13][(data[i++] ^ (crc >>> 16)) & 0xff] ^
      crc32Table[12][data[i++] ^ (crc >>> 24)] ^
      crc32Table[11][data[i++]] ^
      crc32Table[10][data[i++]] ^
      crc32Table[9][data[i++]] ^
      crc32Table[8][data[i++]] ^
      crc32Table[7][data[i++]] ^
      crc32Table[6][data[i++]] ^
      crc32Table[5][data[i++]] ^
      crc32Table[4][data[i++]] ^
      crc32Table[3][data[i++]] ^
      crc32Table[2][data[i++]] ^
      crc32Table[1][data[i++]] ^
      crc32Table[0][data[i++]];

  while (i !== dataLength)
    crc = crc32Table[0][(crc ^ data[i++]) & 0xff] ^ (crc >>> 8);

  return crc ^ -1;
};

const concatBuffers = (...buffers) => {
  const buffer = new uint8Array(
    buffers.reduce((acc, buf) => acc + buf[length], 0),
  );

  buffers.reduce((offset, buf) => {
    buffer.set(buf, offset);
    return offset + buf[length];
  }, 0);

  return buffer;
};

const bytesToString = (bytes) => String.fromCharCode(...bytes);

// prettier-ignore
const reverseTable = [0x0,0x8,0x4,0xc,0x2,0xa,0x6,0xe,0x1,0x9,0x5,0xd,0x3,0xb,0x7,0xf];
const reverse = (val) =>
  (reverseTable[val & 0b1111] << 4) | reverseTable[val >> 4];

class BitReader {
  constructor(data) {
    this._data = data;
    this._pos = data[length] * 8;
  }

  set position(position) {
    this._pos = position;
  }

  get position() {
    return this._pos;
  }

  read(bits) {
    const byte = Math.floor(this._pos / 8);
    const bit = this._pos % 8;
    this._pos -= bits;

    const window =
      (reverse(this._data[byte - 1]) << 8) + reverse(this._data[byte]);

    return (window >> (7 - bit)) & 0xff;
  }
}

/**
 * @todo Old versions of Safari do not support BigInt
 */
const readInt64le = (view, offset) => {
  try {
    return view.getBigInt64(offset, true);
  } catch {
    const sign = view.getUint8(offset + 7) & 0x80 ? -1 : 1;
    let firstPart = view.getUint32(offset, true);
    let secondPart = view.getUint32(offset + 4, true);

    if (sign === -1) {
      firstPart = ~firstPart + 1;
      secondPart = ~secondPart + 1;
    }

    if (secondPart > 0x000fffff) {
      console.warn("This platform does not support BigInt");
    }

    return sign * (firstPart + secondPart * 2 ** 32);
  }
};

export {
  crc8,
  flacCrc16,
  crc32Function,
  reverse,
  concatBuffers,
  bytesToString,
  readInt64le,
  BitReader,
};
