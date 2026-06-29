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

import {
  reserved,
  bad,
  free,
  none,
  sixteenBitCRC,
  rate44100,
  rate48000,
  rate32000,
  rate22050,
  rate24000,
  rate16000,
  rate11025,
  rate12000,
  rate8000,
  monophonic,
  stereo,
  bitDepth,
  channelMode,
  sampleRate,
  bitrate,
  channels,
  isOriginal,
  isPrivate,
  layer,
  length,
  mpegVersion,
  frameLength,
  protection,
  emphasis,
  framePadding,
  isCopyrighted,
  modeExtension,
  description,
  samples,
  subarray,
  readRawData,
  incrementRawData,
  getHeader,
  setHeader,
} from "../../constants.js";
import { bytesToString } from "../../utilities.js";

import ID3v2 from "../../metadata/ID3v2.js";
import CodecHeader from "../CodecHeader.js";

// http://www.mp3-tech.org/programmer/frame_header.html

const bitrateMatrix = {
  // bits | V1,L1 | V1,L2 | V1,L3 | V2,L1 | V2,L2 & L3
  0b00000000: [free, free, free, free, free],
  0b00010000: [32, 32, 32, 32, 8],
  // 0b00100000: [64,   48,  40,  48,  16,],
  // 0b00110000: [96,   56,  48,  56,  24,],
  // 0b01000000: [128,  64,  56,  64,  32,],
  // 0b01010000: [160,  80,  64,  80,  40,],
  // 0b01100000: [192,  96,  80,  96,  48,],
  // 0b01110000: [224, 112,  96, 112,  56,],
  // 0b10000000: [256, 128, 112, 128,  64,],
  // 0b10010000: [288, 160, 128, 144,  80,],
  // 0b10100000: [320, 192, 160, 160,  96,],
  // 0b10110000: [352, 224, 192, 176, 112,],
  // 0b11000000: [384, 256, 224, 192, 128,],
  // 0b11010000: [416, 320, 256, 224, 144,],
  // 0b11100000: [448, 384, 320, 256, 160,],
  0b11110000: [bad, bad, bad, bad, bad],
};

const calcBitrate = (idx, interval, intervalOffset) =>
  8 *
    (((idx + intervalOffset) % interval) + interval) *
    (1 << ((idx + intervalOffset) / interval)) -
  8 * interval * ((interval / 8) | 0);

// generate bitrate matrix
for (let i = 2; i < 15; i++)
  bitrateMatrix[i << 4] = [
    i * 32, //                V1,L1
    calcBitrate(i, 4, 0), //  V1,L2
    calcBitrate(i, 4, -1), // V1,L3
    calcBitrate(i, 8, 4), //  V2,L1
    calcBitrate(i, 8, 0), //  V2,L2 & L3
  ];

const v1Layer1 = 0;
const v1Layer2 = 1;
const v1Layer3 = 2;
const v2Layer1 = 3;
const v2Layer23 = 4;

const bands = "bands ";
const to31 = " to 31";
const layer12ModeExtensions = {
  0b00000000: bands + 4 + to31,
  0b00010000: bands + 8 + to31,
  0b00100000: bands + 12 + to31,
  0b00110000: bands + 16 + to31,
};

const bitrateIndex = "bitrateIndex";
const v2 = "v2";
const v1 = "v1";

const intensityStereo = "Intensity stereo ";
const msStereo = ", MS stereo ";
const on = "on";
const off = "off";
const layer3ModeExtensions = {
  0b00000000: intensityStereo + off + msStereo + off,
  0b00010000: intensityStereo + on + msStereo + off,
  0b00100000: intensityStereo + off + msStereo + on,
  0b00110000: intensityStereo + on + msStereo + on,
};

const layersValues = {
  0b00000000: { [description]: reserved },
  0b00000010: {
    [description]: "Layer III",
    [framePadding]: 1,
    [modeExtension]: layer3ModeExtensions,
    [v1]: {
      [bitrateIndex]: v1Layer3,
      [samples]: 1152,
    },
    [v2]: {
      [bitrateIndex]: v2Layer23,
      [samples]: 576,
    },
  },
  0b00000100: {
    [description]: "Layer II",
    [framePadding]: 1,
    [modeExtension]: layer12ModeExtensions,
    [samples]: 1152,
    [v1]: {
      [bitrateIndex]: v1Layer2,
    },
    [v2]: {
      [bitrateIndex]: v2Layer23,
    },
  },
  0b00000110: {
    [description]: "Layer I",
    [framePadding]: 4,
    [modeExtension]: layer12ModeExtensions,
    [samples]: 384,
    [v1]: {
      [bitrateIndex]: v1Layer1,
    },
    [v2]: {
      [bitrateIndex]: v2Layer1,
    },
  },
};

const mpegVersionDescription = "MPEG Version ";
const isoIec = "ISO/IEC ";
const mpegVersions = {
  0b00000000: {
    [description]: `${mpegVersionDescription}2.5 (later extension of MPEG 2)`,
    [layer]: v2,
    [sampleRate]: {
      0b00000000: rate11025,
      0b00000100: rate12000,
      0b00001000: rate8000,
      0b00001100: reserved,
    },
  },
  0b00001000: { [description]: reserved },
  0b00010000: {
    [description]: `${mpegVersionDescription}2 (${isoIec}13818-3)`,
    [layer]: v2,
    [sampleRate]: {
      0b00000000: rate22050,
      0b00000100: rate24000,
      0b00001000: rate16000,
      0b00001100: reserved,
    },
  },
  0b00011000: {
    [description]: `${mpegVersionDescription}1 (${isoIec}11172-3)`,
    [layer]: v1,
    [sampleRate]: {
      0b00000000: rate44100,
      0b00000100: rate48000,
      0b00001000: rate32000,
      0b00001100: reserved,
    },
  },
  length,
};

const protectionValues = {
  0b00000000: sixteenBitCRC,
  0b00000001: none,
};

const emphasisValues = {
  0b00000000: none,
  0b00000001: "50/15 ms",
  0b00000010: reserved,
  0b00000011: "CCIT J.17",
};

const channelModes = {
  0b00000000: { [channels]: 2, [description]: stereo },
  0b01000000: { [channels]: 2, [description]: "joint " + stereo },
  0b10000000: { [channels]: 2, [description]: "dual channel" },
  0b11000000: { [channels]: 1, [description]: monophonic },
};

export default class MPEGHeader extends CodecHeader {
  static *[getHeader](codecParser, headerCache, readOffset) {
    const header = {};

    // check for id3 header
    const id3v2Header = yield* ID3v2.getID3v2Header(
      codecParser,
      headerCache,
      readOffset,
    );

    if (id3v2Header) {
      // throw away the data. id3 parsing is not implemented yet.
      yield* codecParser[readRawData](id3v2Header[length], readOffset);
      codecParser[incrementRawData](id3v2Header[length]);
    }

    // Must be at least four bytes.
    const data = yield* codecParser[readRawData](4, readOffset);

    // Check header cache
    const key = bytesToString(data[subarray](0, 4));
    const cachedHeader = headerCache[getHeader](key);
    if (cachedHeader) return new MPEGHeader(cachedHeader);

    // Frame sync (all bits must be set): `11111111|111`:
    if (data[0] !== 0xff || data[1] < 0xe0) return null;

    // Byte (2 of 4)
    // * `111BBCCD`
    // * `...BB...`: MPEG Audio version ID
    // * `.....CC.`: Layer description
    // * `.......D`: Protection bit (0 - Protected by CRC (16bit CRC follows header), 1 = Not protected)

    // Mpeg version (1, 2, 2.5)
    const mpegVersionValues = mpegVersions[data[1] & 0b00011000];
    if (mpegVersionValues[description] === reserved) return null;

    // Layer (I, II, III)
    const layerBits = data[1] & 0b00000110;
    if (layersValues[layerBits][description] === reserved) return null;
    const layerValues = {
      ...layersValues[layerBits],
      ...layersValues[layerBits][mpegVersionValues[layer]],
    };

    header[mpegVersion] = mpegVersionValues[description];
    header[layer] = layerValues[description];
    header[samples] = layerValues[samples];
    header[protection] = protectionValues[data[1] & 0b00000001];

    header[length] = 4;

    // Byte (3 of 4)
    // * `EEEEFFGH`
    // * `EEEE....`: Bitrate index. 1111 is invalid, everything else is accepted
    // * `....FF..`: Sample rate
    // * `......G.`: Padding bit, 0=frame not padded, 1=frame padded
    // * `.......H`: Private bit.
    header[bitrate] =
      bitrateMatrix[data[2] & 0b11110000][layerValues[bitrateIndex]];
    if (header[bitrate] === bad) return null;

    header[sampleRate] = mpegVersionValues[sampleRate][data[2] & 0b00001100];
    if (header[sampleRate] === reserved) return null;

    header[framePadding] = data[2] & 0b00000010 && layerValues[framePadding];
    header[isPrivate] = !!(data[2] & 0b00000001);

    header[frameLength] = Math.floor(
      (125 * header[bitrate] * header[samples]) / header[sampleRate] +
        header[framePadding],
    );
    if (!header[frameLength]) return null;

    // Byte (4 of 4)
    // * `IIJJKLMM`
    // * `II......`: Channel mode
    // * `..JJ....`: Mode extension (only if joint stereo)
    // * `....K...`: Copyright
    // * `.....L..`: Original
    // * `......MM`: Emphasis
    const channelModeBits = data[3] & 0b11000000;
    header[channelMode] = channelModes[channelModeBits][description];
    header[channels] = channelModes[channelModeBits][channels];

    header[modeExtension] = layerValues[modeExtension][data[3] & 0b00110000];
    header[isCopyrighted] = !!(data[3] & 0b00001000);
    header[isOriginal] = !!(data[3] & 0b00000100);

    header[emphasis] = emphasisValues[data[3] & 0b00000011];
    if (header[emphasis] === reserved) return null;

    header[bitDepth] = 16;

    // set header cache
    {
      const { length, frameLength, samples, ...codecUpdateFields } = header;

      headerCache[setHeader](key, header, codecUpdateFields);
    }
    return new MPEGHeader(header);
  }

  /**
   * @private
   * Call MPEGHeader.getHeader(Array<Uint8>) to get instance
   */
  constructor(header) {
    super(header);

    this[bitrate] = header[bitrate];
    this[emphasis] = header[emphasis];
    this[framePadding] = header[framePadding];
    this[isCopyrighted] = header[isCopyrighted];
    this[isOriginal] = header[isOriginal];
    this[isPrivate] = header[isPrivate];
    this[layer] = header[layer];
    this[modeExtension] = header[modeExtension];
    this[mpegVersion] = header[mpegVersion];
    this[protection] = header[protection];
  }
}
