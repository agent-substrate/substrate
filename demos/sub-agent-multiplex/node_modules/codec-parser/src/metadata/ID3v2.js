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

// https://id3.org/Developer%20Information

import { length, version, readRawData } from "../constants.js";

const unsynchronizationFlag = "unsynchronizationFlag";
const extendedHeaderFlag = "extendedHeaderFlag";
const experimentalFlag = "experimentalFlag";
const footerPresent = "footerPresent";

export default class ID3v2 {
  static *getID3v2Header(codecParser, headerCache, readOffset) {
    const headerLength = 10;
    const header = {};

    let data = yield* codecParser[readRawData](3, readOffset);
    // Byte (0-2 of 9)
    // ID3
    if (data[0] !== 0x49 || data[1] !== 0x44 || data[2] !== 0x33) return null;

    data = yield* codecParser[readRawData](headerLength, readOffset);

    // Byte (3-4 of 9)
    // * `BBBBBBBB|........`: Major version
    // * `........|BBBBBBBB`: Minor version
    header[version] = `id3v2.${data[3]}.${data[4]}`;

    // Byte (5 of 9)
    // * `....0000.: Zeros (flags not implemented yet)
    if (data[5] & 0b00001111) return null;

    // Byte (5 of 9)
    // * `CDEF0000`: Flags
    // * `C.......`: Unsynchronisation (indicates whether or not unsynchronisation is used)
    // * `.D......`: Extended header (indicates whether or not the header is followed by an extended header)
    // * `..E.....`: Experimental indicator (indicates whether or not the tag is in an experimental stage)
    // * `...F....`: Footer present (indicates that a footer is present at the very end of the tag)
    header[unsynchronizationFlag] = !!(data[5] & 0b10000000);
    header[extendedHeaderFlag] = !!(data[5] & 0b01000000);
    header[experimentalFlag] = !!(data[5] & 0b00100000);
    header[footerPresent] = !!(data[5] & 0b00010000);

    // Byte (6-9 of 9)
    // * `0.......|0.......|0.......|0.......`: Zeros
    if (
      data[6] & 0b10000000 ||
      data[7] & 0b10000000 ||
      data[8] & 0b10000000 ||
      data[9] & 0b10000000
    )
      return null;

    // Byte (6-9 of 9)
    // * `.FFFFFFF|.FFFFFFF|.FFFFFFF|.FFFFFFF`: Tag Length
    // The ID3v2 tag size is encoded with four bytes where the most significant bit (bit 7)
    // is set to zero in every byte, making a total of 28 bits. The zeroed bits are ignored,
    // so a 257 bytes long tag is represented as $00 00 02 01.
    const dataLength =
      (data[6] << 21) | (data[7] << 14) | (data[8] << 7) | data[9];

    header[length] = headerLength + dataLength;

    return new ID3v2(header);
  }

  constructor(header) {
    this[version] = header[version];
    this[unsynchronizationFlag] = header[unsynchronizationFlag];
    this[extendedHeaderFlag] = header[extendedHeaderFlag];
    this[experimentalFlag] = header[experimentalFlag];
    this[footerPresent] = header[footerPresent];
    this[length] = header[length];
  }
}
