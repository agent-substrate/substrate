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
import { bytesToString, concatBuffers } from "../../utilities.js";
import {
  header,
  pageSequenceNumber,
  pageSegmentBytes,
  pageSegmentTable,
  codec,
  data,
  length,
  samples,
  segments,
  subarray,
  vorbis,
  mapFrameStats,
  logWarning,
  fixedLengthFrameSync,
  parseFrame,
  parseOggPage,
  reset,
  uint8Array,
  isLastPage,
  streamSerialNumber,
  absoluteGranulePosition,
} from "../../constants.js";

import Parser from "../../codecs/Parser.js";
import OggPage from "./OggPage.js";
import OggPageHeader from "./OggPageHeader.js";

import FLACParser from "../../codecs/flac/FLACParser.js";
import OpusParser from "../../codecs/opus/OpusParser.js";
import VorbisParser from "../../codecs/vorbis/VorbisParser.js";

class OggStream {
  constructor(codecParser, headerCache, onCodec) {
    this._codecParser = codecParser;
    this._headerCache = headerCache;
    this._onCodec = onCodec;

    this._continuedPacket = new uint8Array();
    this._codec = null;
    this._isSupported = null;
    this._previousAbsoluteGranulePosition = null;
  }

  get [codec]() {
    return this._codec || "";
  }

  _updateCodec(codec, Parser) {
    if (this._codec !== codec) {
      this._headerCache[reset]();
      this._parser = new Parser(
        this._codecParser,
        this._headerCache,
        this._onCodec,
      );
      this._codec = codec;
    }
  }

  _checkCodecSupport({ data }) {
    const idString = bytesToString(data[subarray](0, 8));

    switch (idString) {
      case "fishead\0":
        return false; // ignore ogg skeleton packets
      case "OpusHead":
        this._updateCodec("opus", OpusParser);
        return true;
      case /^\x7fFLAC/.test(idString) && idString:
        this._updateCodec("flac", FLACParser);
        return true;
      case /^\x01vorbis/.test(idString) && idString:
        this._updateCodec(vorbis, VorbisParser);
        return true;
      default:
        return false;
    }
  }

  _checkPageSequenceNumber(oggPage) {
    if (
      oggPage[pageSequenceNumber] !== this._pageSequenceNumber + 1 &&
      this._pageSequenceNumber > 1 &&
      oggPage[pageSequenceNumber] > 1
    ) {
      this._codecParser[logWarning](
        "Unexpected gap in Ogg Page Sequence Number.",
        `Expected: ${this._pageSequenceNumber + 1}, Got: ${
          oggPage[pageSequenceNumber]
        }`,
      );
    }

    this._pageSequenceNumber = oggPage[pageSequenceNumber];
  }

  _parsePage(oggPage) {
    if (this._isSupported === null) {
      this._pageSequenceNumber = oggPage[pageSequenceNumber];
      this._isSupported = this._checkCodecSupport(oggPage);
    }

    this._checkPageSequenceNumber(oggPage);

    const oggPageStore = frameStore.get(oggPage);
    const headerData = headerStore.get(oggPageStore[header]);

    let offset = 0;
    oggPageStore[segments] = headerData[pageSegmentTable].map((segmentLength) =>
      oggPage[data][subarray](offset, (offset += segmentLength)),
    );

    // prepend any existing continued packet data
    if (this._continuedPacket[length]) {
      oggPageStore[segments][0] = concatBuffers(
        this._continuedPacket,
        oggPageStore[segments][0],
      );

      this._continuedPacket = new uint8Array();
    }

    // save any new continued packet data
    if (
      headerData[pageSegmentBytes][headerData[pageSegmentBytes][length] - 1] ===
      0xff
    ) {
      this._continuedPacket = concatBuffers(
        this._continuedPacket,
        oggPageStore[segments].pop(),
      );
    }

    // set total samples in this ogg page
    if (this._previousAbsoluteGranulePosition !== null) {
      oggPage[samples] = Number(
        oggPage[absoluteGranulePosition] -
          this._previousAbsoluteGranulePosition,
      );
    }

    this._previousAbsoluteGranulePosition = oggPage[absoluteGranulePosition];

    if (this._isSupported) {
      const frame = this._parser[parseOggPage](oggPage);
      this._codecParser[mapFrameStats](frame);

      return frame;
    } else {
      return oggPage;
    }
  }
}

export default class OggParser extends Parser {
  constructor(codecParser, headerCache, onCodec) {
    super(codecParser, headerCache);

    this._onCodec = onCodec;
    this.Frame = OggPage;
    this.Header = OggPageHeader;

    this._streams = new Map();
    this._currentSerialNumber = null;
  }

  get [codec]() {
    const oggStream = this._streams.get(this._currentSerialNumber);

    return oggStream ? oggStream.codec : "";
  }

  *[parseFrame]() {
    const oggPage = yield* this[fixedLengthFrameSync](true);
    this._currentSerialNumber = oggPage[streamSerialNumber];

    let oggStream = this._streams.get(this._currentSerialNumber);
    if (!oggStream) {
      oggStream = new OggStream(
        this._codecParser,
        this._headerCache,
        this._onCodec,
      );
      this._streams.set(this._currentSerialNumber, oggStream);
    }

    if (oggPage[isLastPage]) this._streams.delete(this._currentSerialNumber);

    return oggStream._parsePage(oggPage);
  }
}
