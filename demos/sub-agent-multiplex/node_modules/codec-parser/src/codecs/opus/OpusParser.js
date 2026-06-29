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

import { frameStore } from "../../globals.js";
import {
  codecFrames,
  data,
  pageSequenceNumber,
  codec,
  segments,
  logError,
  parseOggPage,
  enable,
  getHeaderFromUint8Array,
  preSkip,
  frameSize,
  frameCount,
  sampleRate,
} from "../../constants.js";
import Parser from "../Parser.js";
import OpusFrame from "./OpusFrame.js";
import OpusHeader from "./OpusHeader.js";

export default class OpusParser extends Parser {
  constructor(codecParser, headerCache, onCodec) {
    super(codecParser, headerCache);
    this.Frame = OpusFrame;
    this.Header = OpusHeader;

    onCodec(this[codec]);
    this._identificationHeader = null;
    this._preSkipRemaining = null;
  }

  get [codec]() {
    return "opus";
  }

  /**
   * @todo implement continued page support
   */
  [parseOggPage](oggPage) {
    if (oggPage[pageSequenceNumber] === 0) {
      // Identification header

      this._headerCache[enable]();
      this._identificationHeader = oggPage[data];
    } else if (oggPage[pageSequenceNumber] === 1) {
      // OpusTags
    } else {
      oggPage[codecFrames] = frameStore
        .get(oggPage)
        [segments].map((segment) => {
          const header = OpusHeader[getHeaderFromUint8Array](
            this._identificationHeader,
            segment,
            this._headerCache,
          );

          if (header) {
            if (this._preSkipRemaining === null)
              this._preSkipRemaining = header[preSkip];

            let samples =
              ((header[frameSize] * header[frameCount]) / 1000) *
              header[sampleRate];

            if (this._preSkipRemaining > 0) {
              this._preSkipRemaining -= samples;
              samples =
                this._preSkipRemaining < 0 ? -this._preSkipRemaining : 0;
            }

            return new OpusFrame(segment, header, samples);
          }

          this._codecParser[logError](
            "Failed to parse Ogg Opus Header",
            "Not a valid Ogg Opus file",
          );
        });
    }

    return oggPage;
  }
}
