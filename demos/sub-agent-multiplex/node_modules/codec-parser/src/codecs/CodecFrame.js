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

import { frameStore, headerStore } from "../globals.js";
import {
  sampleRate,
  length,
  frameNumber,
  header,
  samples,
  duration,
  totalBytesOut,
  totalSamples,
  totalDuration,
  frameLength,
  subarray,
  readRawData,
  getFrame,
  getHeader,
} from "../constants.js";
import Frame from "../containers/Frame.js";

export default class CodecFrame extends Frame {
  static *[getFrame](Header, Frame, codecParser, headerCache, readOffset) {
    const headerValue = yield* Header[getHeader](
      codecParser,
      headerCache,
      readOffset,
    );

    if (headerValue) {
      const frameLengthValue = headerStore.get(headerValue)[frameLength];
      const samplesValue = headerStore.get(headerValue)[samples];

      const frame = (yield* codecParser[readRawData](
        frameLengthValue,
        readOffset,
      ))[subarray](0, frameLengthValue);

      return new Frame(headerValue, frame, samplesValue);
    } else {
      return null;
    }
  }

  constructor(headerValue, dataValue, samplesValue) {
    super(headerValue, dataValue);

    this[header] = headerValue;
    this[samples] = samplesValue;
    this[duration] = (samplesValue / headerValue[sampleRate]) * 1000;
    this[frameNumber] = null;
    this[totalBytesOut] = null;
    this[totalSamples] = null;
    this[totalDuration] = null;

    frameStore.get(this)[length] = dataValue[length];
  }
}
