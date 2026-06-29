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

import { headerStore, frameStore } from "../../globals.js";
import {
  codecFrames,
  rawData,
  absoluteGranulePosition,
  crc32,
  duration,
  isContinuedPacket,
  isFirstPage,
  isLastPage,
  pageSequenceNumber,
  samples,
  streamSerialNumber,
  length,
  pageChecksum,
  frameLength,
  subarray,
  readRawData,
  getFrame,
  getHeader,
} from "../../constants.js";

import Frame from "../Frame.js";
import OggPageHeader from "./OggPageHeader.js";

export default class OggPage extends Frame {
  static *[getFrame](codecParser, headerCache, readOffset) {
    const header = yield* OggPageHeader[getHeader](
      codecParser,
      headerCache,
      readOffset,
    );

    if (header) {
      const frameLengthValue = headerStore.get(header)[frameLength];
      const headerLength = headerStore.get(header)[length];
      const totalLength = headerLength + frameLengthValue;

      const rawDataValue = (yield* codecParser[readRawData](totalLength, 0))[
        subarray
      ](0, totalLength);

      const frame = rawDataValue[subarray](headerLength, totalLength);

      return new OggPage(header, frame, rawDataValue);
    } else {
      return null;
    }
  }

  constructor(header, frame, rawDataValue) {
    super(header, frame);

    frameStore.get(this)[length] = rawDataValue[length];

    this[codecFrames] = [];
    this[rawData] = rawDataValue;
    this[absoluteGranulePosition] = header[absoluteGranulePosition];
    this[crc32] = header[pageChecksum];
    this[duration] = 0;
    this[isContinuedPacket] = header[isContinuedPacket];
    this[isFirstPage] = header[isFirstPage];
    this[isLastPage] = header[isLastPage];
    this[pageSequenceNumber] = header[pageSequenceNumber];
    this[samples] = 0;
    this[streamSerialNumber] = header[streamSerialNumber];
  }
}
