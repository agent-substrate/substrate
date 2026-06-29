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

import { crc32Function, concatBuffers } from "./utilities.js";
import {
  header,
  sampleRate,
  bitrate,
  length,
  frameNumber,
  data,
  samples,
  codec,
  codecFrames,
  totalBytesOut,
  totalSamples,
  totalDuration,
  crc32,
  duration,
  subarray,
  readRawData,
  incrementRawData,
  mapCodecFrameStats,
  mapFrameStats,
  logWarning,
  logError,
  parseFrame,
  checkCodecUpdate,
  reset,
  isLastPage,
} from "./constants.js";
import HeaderCache from "./codecs/HeaderCache.js";
import MPEGParser from "./codecs/mpeg/MPEGParser.js";
import AACParser from "./codecs/aac/AACParser.js";
import FLACParser from "./codecs/flac/FLACParser.js";
import OggParser from "./containers/ogg/OggParser.js";

const noOp = () => {};

export default class CodecParser {
  constructor(
    mimeType,
    {
      onCodec,
      onCodecHeader,
      onCodecUpdate,
      enableLogging = false,
      enableFrameCRC32 = true,
    } = {},
  ) {
    this._inputMimeType = mimeType;
    this._onCodec = onCodec || noOp;
    this._onCodecHeader = onCodecHeader || noOp;
    this._onCodecUpdate = onCodecUpdate;
    this._enableLogging = enableLogging;
    this._crc32 = enableFrameCRC32 ? crc32Function : noOp;

    this[reset]();
  }

  /**
   * @public
   * @returns The detected codec
   */
  get [codec]() {
    return this._parser ? this._parser[codec] : "";
  }

  [reset]() {
    this._headerCache = new HeaderCache(
      this._onCodecHeader,
      this._onCodecUpdate,
    );

    this._generator = this._getGenerator();
    this._generator.next();
  }

  /**
   * @public
   * @description Generator function that yields any buffered CodecFrames and resets the CodecParser
   * @returns {Iterable<CodecFrame|OggPage>} Iterator that operates over the codec data.
   * @yields {CodecFrame|OggPage} Parsed codec or ogg page data
   */
  *flush() {
    this._flushing = true;

    for (let i = this._generator.next(); i.value; i = this._generator.next()) {
      yield i.value;
    }

    this._flushing = false;

    this[reset]();
  }

  /**
   * @public
   * @description Generator function takes in a Uint8Array of data and returns a CodecFrame from the data for each iteration
   * @param {Uint8Array} chunk Next chunk of codec data to read
   * @returns {Iterable<CodecFrame|OggPage>} Iterator that operates over the codec data.
   * @yields {CodecFrame|OggPage} Parsed codec or ogg page data
   */
  *parseChunk(chunk) {
    for (
      let i = this._generator.next(chunk);
      i.value;
      i = this._generator.next()
    ) {
      yield i.value;
    }
  }

  /**
   * @public
   * @description Parses an entire file and returns all of the contained frames.
   * @param {Uint8Array} fileData Coded data to read
   * @returns {Array<CodecFrame|OggPage>} CodecFrames
   */
  parseAll(fileData) {
    return [...this.parseChunk(fileData), ...this.flush()];
  }

  /**
   * @private
   */
  *_getGenerator() {
    if (this._inputMimeType.match(/aac/)) {
      this._parser = new AACParser(this, this._headerCache, this._onCodec);
    } else if (this._inputMimeType.match(/mpeg/)) {
      this._parser = new MPEGParser(this, this._headerCache, this._onCodec);
    } else if (this._inputMimeType.match(/flac/)) {
      this._parser = new FLACParser(this, this._headerCache, this._onCodec);
    } else if (this._inputMimeType.match(/ogg/)) {
      this._parser = new OggParser(this, this._headerCache, this._onCodec);
    } else {
      throw new Error(`Unsupported Codec ${mimeType}`);
    }

    this._frameNumber = 0;
    this._currentReadPosition = 0;
    this._totalBytesIn = 0;
    this._totalBytesOut = 0;
    this._totalSamples = 0;
    this._sampleRate = undefined;

    this._rawData = new Uint8Array(0);

    // start parsing out frames
    while (true) {
      const frame = yield* this._parser[parseFrame]();
      if (frame) yield frame;
    }
  }

  /**
   * @protected
   * @param {number} minSize Minimum bytes to have present in buffer
   * @returns {Uint8Array} rawData
   */
  *[readRawData](minSize = 0, readOffset = 0) {
    let rawData;

    while (this._rawData[length] <= minSize + readOffset) {
      rawData = yield;

      if (this._flushing) return this._rawData[subarray](readOffset);

      if (rawData) {
        this._totalBytesIn += rawData[length];
        this._rawData = concatBuffers(this._rawData, rawData);
      }
    }

    return this._rawData[subarray](readOffset);
  }

  /**
   * @protected
   * @param {number} increment Bytes to increment codec data
   */
  [incrementRawData](increment) {
    this._currentReadPosition += increment;
    this._rawData = this._rawData[subarray](increment);
  }

  /**
   * @protected
   */
  [mapCodecFrameStats](frame) {
    this._sampleRate = frame[header][sampleRate];

    frame[header][bitrate] =
      frame[duration] > 0
        ? Math.round(frame[data][length] / frame[duration]) * 8
        : 0;
    frame[frameNumber] = this._frameNumber++;
    frame[totalBytesOut] = this._totalBytesOut;
    frame[totalSamples] = this._totalSamples;
    frame[totalDuration] = (this._totalSamples / this._sampleRate) * 1000;
    frame[crc32] = this._crc32(frame[data]);

    this._headerCache[checkCodecUpdate](
      frame[header][bitrate],
      frame[totalDuration],
    );

    this._totalBytesOut += frame[data][length];
    this._totalSamples += frame[samples];
  }

  /**
   * @protected
   */
  [mapFrameStats](frame) {
    if (frame[codecFrames]) {
      // Ogg container
      if (frame[isLastPage]) {
        // cut any excess samples that fall outside of the absolute granule position
        // some streams put invalid data in absolute granule position, so only do this
        // for the end of the stream
        let absoluteGranulePositionSamples = frame[samples];

        frame[codecFrames].forEach((codecFrame) => {
          const untrimmedCodecSamples = codecFrame[samples];

          if (absoluteGranulePositionSamples < untrimmedCodecSamples) {
            codecFrame[samples] =
              absoluteGranulePositionSamples > 0
                ? absoluteGranulePositionSamples
                : 0;
            codecFrame[duration] =
              (codecFrame[samples] / codecFrame[header][sampleRate]) * 1000;
          }

          absoluteGranulePositionSamples -= untrimmedCodecSamples;

          this[mapCodecFrameStats](codecFrame);
        });
      } else {
        frame[samples] = 0;
        frame[codecFrames].forEach((codecFrame) => {
          frame[samples] += codecFrame[samples];
          this[mapCodecFrameStats](codecFrame);
        });
      }

      frame[duration] = (frame[samples] / this._sampleRate) * 1000 || 0;
      frame[totalSamples] = this._totalSamples;
      frame[totalDuration] =
        (this._totalSamples / this._sampleRate) * 1000 || 0;
      frame[totalBytesOut] = this._totalBytesOut;
    } else {
      this[mapCodecFrameStats](frame);
    }
  }

  /**
   * @private
   */
  _log(logger, messages) {
    if (this._enableLogging) {
      const stats = [
        `${codec}:         ${this[codec]}`,
        `inputMimeType: ${this._inputMimeType}`,
        `readPosition:  ${this._currentReadPosition}`,
        `totalBytesIn:  ${this._totalBytesIn}`,
        `${totalBytesOut}: ${this._totalBytesOut}`,
      ];

      const width = Math.max(...stats.map((s) => s[length]));

      messages.push(
        `--stats--${"-".repeat(width - 9)}`,
        ...stats,
        "-".repeat(width),
      );

      logger(
        "codec-parser",
        messages.reduce((acc, message) => acc + "\n  " + message, ""),
      );
    }
  }

  /**
   * @protected
   */
  [logWarning](...messages) {
    this._log(console.warn, messages);
  }

  /**
   * @protected
   */
  [logError](...messages) {
    this._log(console.error, messages);
  }
}
