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

/*
https://xiph.org/ogg/doc/framing.html

AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA BBBBBBBB 00000CDE

(LSB)                                                             (MSB)
FFFFFFFF FFFFFFFF FFFFFFFF FFFFFFFF FFFFFFFF FFFFFFFF FFFFFFFF FFFFFFFF
GGGGGGGG GGGGGGGG GGGGGGGG GGGGGGGG
HHHHHHHH HHHHHHHH HHHHHHHH HHHHHHHH
IIIIIIII IIIIIIII IIIIIIII IIIIIIII

JJJJJJJJ
LLLLLLLL...

Ogg Page Header
Letter  Length (bits)  Description
A   32  0x4f676753, "OggS"
B   8   stream_structure_version
C   1   (0 no, 1 yes) last page of logical bitstream (eos)
D   1   (0 no, 1 yes) first page of logical bitstream (bos)
E   1   (0 no, 1 yes) continued packet

F   64  absolute granule position
G   32  stream serial number
H   32  page sequence no
I   32  page checksum
J   8   Number of page segments in the segment table
L   n   Segment table (n=page_segments+26).
        Segment table values sum to the total length of the packet.
        Last value is always < 0xFF. Last lacing value will be 0x00 if evenly divisible by 0xFF.
        
*/

import { headerStore } from "../../globals.js";
import {
  absoluteGranulePosition,
  isContinuedPacket,
  isFirstPage,
  isLastPage,
  pageSequenceNumber,
  streamSerialNumber,
  length,
  pageChecksum,
  frameLength,
  streamStructureVersion,
  pageSegmentTable,
  pageSegmentBytes,
  buffer,
  subarray,
  readRawData,
  getHeader,
  uint8Array,
  dataView,
} from "../../constants.js";
import { readInt64le } from "../../utilities.js";

export default class OggPageHeader {
  static *[getHeader](codecParser, headerCache, readOffset) {
    const header = {};

    // Must be at least 28 bytes.
    let data = yield* codecParser[readRawData](28, readOffset);

    // Bytes (1-4 of 28)
    // Frame sync (must equal OggS): `AAAAAAAA|AAAAAAAA|AAAAAAAA|AAAAAAAA`:
    if (
      data[0] !== 0x4f || // O
      data[1] !== 0x67 || // g
      data[2] !== 0x67 || // g
      data[3] !== 0x53 //    S
    ) {
      return null;
    }

    // Byte (5 of 28)
    // * `BBBBBBBB`: stream_structure_version
    header[streamStructureVersion] = data[4];

    // Byte (6 of 28)
    // * `00000CDE`
    // * `00000...`: All zeros
    // * `.....C..`: (0 no, 1 yes) last page of logical bitstream (eos)
    // * `......D.`: (0 no, 1 yes) first page of logical bitstream (bos)
    // * `.......E`: (0 no, 1 yes) continued packet
    const zeros = data[5] & 0b11111000;
    if (zeros) return null;

    header[isLastPage] = !!(data[5] & 0b00000100);
    header[isFirstPage] = !!(data[5] & 0b00000010);
    header[isContinuedPacket] = !!(data[5] & 0b00000001);

    const view = new dataView(uint8Array.from(data[subarray](0, 28))[buffer]);

    // Byte (7-14 of 28)
    // * `FFFFFFFF|FFFFFFFF|FFFFFFFF|FFFFFFFF|FFFFFFFF|FFFFFFFF|FFFFFFFF|FFFFFFFF`
    // * Absolute Granule Position
    header[absoluteGranulePosition] = readInt64le(view, 6);

    // Byte (15-18 of 28)
    // * `GGGGGGGG|GGGGGGGG|GGGGGGGG|GGGGGGGG`
    // * Stream Serial Number
    header[streamSerialNumber] = view.getInt32(14, true);

    // Byte (19-22 of 28)
    // * `HHHHHHHH|HHHHHHHH|HHHHHHHH|HHHHHHHH`
    // * Page Sequence Number
    header[pageSequenceNumber] = view.getInt32(18, true);

    // Byte (23-26 of 28)
    // * `IIIIIIII|IIIIIIII|IIIIIIII|IIIIIIII`
    // * Page Checksum
    header[pageChecksum] = view.getInt32(22, true);

    // Byte (27 of 28)
    // * `JJJJJJJJ`: Number of page segments in the segment table
    const pageSegmentTableLength = data[26];
    header[length] = pageSegmentTableLength + 27;

    data = yield* codecParser[readRawData](header[length], readOffset); // read in the page segment table

    header[frameLength] = 0;
    header[pageSegmentTable] = [];
    header[pageSegmentBytes] = uint8Array.from(
      data[subarray](27, header[length]),
    );

    for (let i = 0, segmentLength = 0; i < pageSegmentTableLength; i++) {
      const segmentByte = header[pageSegmentBytes][i];

      header[frameLength] += segmentByte;
      segmentLength += segmentByte;

      if (segmentByte !== 0xff || i === pageSegmentTableLength - 1) {
        header[pageSegmentTable].push(segmentLength);
        segmentLength = 0;
      }
    }

    return new OggPageHeader(header);
  }

  /**
   * @private
   * Call OggPageHeader.getHeader(Array<Uint8>) to get instance
   */
  constructor(header) {
    headerStore.set(this, header);

    this[absoluteGranulePosition] = header[absoluteGranulePosition];
    this[isContinuedPacket] = header[isContinuedPacket];
    this[isFirstPage] = header[isFirstPage];
    this[isLastPage] = header[isLastPage];
    this[pageSegmentTable] = header[pageSegmentTable];
    this[pageSequenceNumber] = header[pageSequenceNumber];
    this[pageChecksum] = header[pageChecksum];
    this[streamSerialNumber] = header[streamSerialNumber];
  }
}
