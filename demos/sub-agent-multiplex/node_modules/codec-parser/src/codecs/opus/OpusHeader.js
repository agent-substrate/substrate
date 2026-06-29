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
https://tools.ietf.org/html/rfc7845.html
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      'O'      |      'p'      |      'u'      |      's'      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      'H'      |      'e'      |      'a'      |      'd'      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  Version = 1  | Channel Count |           Pre-skip            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     Input Sample Rate (Hz)                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   Output Gain (Q7.8 in dB)    | Mapping Family|               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+               :
|                                                               |
:               Optional Channel Mapping Table...               :
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

Letter  Length (bits)  Description
A  64  Magic Signature - OpusHead
B  8   Version number - 00000001
C  8   Output channel count (unsigned)
D  16  Pre-skip (unsigned, little endian)
E  32  Sample rate (unsigned, little endian)
F  16  Output Gain (signed, little endian)
G  8   Channel Mapping family (unsigned)

// if(channel mapping !== 0)
H  8   Stream count (unsigned)
I  8   Coupled Stream Count (unsigned)
J  8*C Channel Mapping
*/

import {
  rate48000,
  vorbisOpusChannelMapping,
  bitDepth,
  channelMode,
  sampleRate,
  channels,
  length,
  bandwidth,
  channelMappingFamily,
  channelMappingTable,
  coupledStreamCount,
  frameCount,
  frameSize,
  hasOpusPadding,
  inputSampleRate,
  isVbr,
  mode,
  outputGain,
  preSkip,
  streamCount,
  data,
  buffer,
  subarray,
  getHeader,
  setHeader,
  getHeaderFromUint8Array,
  uint8Array,
  dataView,
} from "../../constants.js";
import { bytesToString } from "../../utilities.js";

import CodecHeader from "../CodecHeader.js";

/* prettier-ignore */
const channelMappingFamilies = {
  0b00000000: vorbisOpusChannelMapping.slice(0,2),
    /*
    0: "monophonic (mono)"
    1: "stereo (left, right)"
    */
  0b00000001: vorbisOpusChannelMapping
    /*
    0: "monophonic (mono)"
    1: "stereo (left, right)"
    2: "linear surround (left, center, right)"
    3: "quadraphonic (front left, front right, rear left, rear right)"
    4: "5.0 surround (front left, front center, front right, rear left, rear right)"
    5: "5.1 surround (front left, front center, front right, rear left, rear right, LFE)"
    6: "6.1 surround (front left, front center, front right, side left, side right, rear center, LFE)"
    7: "7.1 surround (front left, front center, front right, side left, side right, rear left, rear right, LFE)"
    */
  // additional channel mappings are user defined
};

const silkOnly = "SILK-only";
const celtOnly = "CELT-only";
const hybrid = "Hybrid";

const narrowBand = "narrowband";
const mediumBand = "medium-band";
const wideBand = "wideband";
const superWideBand = "super-wideband";
const fullBand = "fullband";

//  0 1 2 3 4 5 6 7
// +-+-+-+-+-+-+-+-+
// | config  |s| c |
// +-+-+-+-+-+-+-+-+
// prettier-ignore
const configTable = {
  0b00000000: { [mode]: silkOnly, [bandwidth]: narrowBand, [frameSize]: 10 },
  0b00001000: { [mode]: silkOnly, [bandwidth]: narrowBand, [frameSize]: 20 },
  0b00010000: { [mode]: silkOnly, [bandwidth]: narrowBand, [frameSize]: 40 },
  0b00011000: { [mode]: silkOnly, [bandwidth]: narrowBand, [frameSize]: 60 },
  0b00100000: { [mode]: silkOnly, [bandwidth]: mediumBand, [frameSize]: 10 },
  0b00101000: { [mode]: silkOnly, [bandwidth]: mediumBand, [frameSize]: 20 },
  0b00110000: { [mode]: silkOnly, [bandwidth]: mediumBand, [frameSize]: 40 },
  0b00111000: { [mode]: silkOnly, [bandwidth]: mediumBand, [frameSize]: 60 },
  0b01000000: { [mode]: silkOnly, [bandwidth]: wideBand, [frameSize]: 10 },
  0b01001000: { [mode]: silkOnly, [bandwidth]: wideBand, [frameSize]: 20 },
  0b01010000: { [mode]: silkOnly, [bandwidth]: wideBand, [frameSize]: 40 },
  0b01011000: { [mode]: silkOnly, [bandwidth]: wideBand, [frameSize]: 60 },
  0b01100000: { [mode]: hybrid, [bandwidth]: superWideBand, [frameSize]: 10 },
  0b01101000: { [mode]: hybrid, [bandwidth]: superWideBand, [frameSize]: 20 },
  0b01110000: { [mode]: hybrid, [bandwidth]: fullBand, [frameSize]: 10 },
  0b01111000: { [mode]: hybrid, [bandwidth]: fullBand, [frameSize]: 20 },
  0b10000000: { [mode]: celtOnly, [bandwidth]: narrowBand, [frameSize]: 2.5 },
  0b10001000: { [mode]: celtOnly, [bandwidth]: narrowBand, [frameSize]: 5 },
  0b10010000: { [mode]: celtOnly, [bandwidth]: narrowBand, [frameSize]: 10 },
  0b10011000: { [mode]: celtOnly, [bandwidth]: narrowBand, [frameSize]: 20 },
  0b10100000: { [mode]: celtOnly, [bandwidth]: wideBand, [frameSize]: 2.5 },
  0b10101000: { [mode]: celtOnly, [bandwidth]: wideBand, [frameSize]: 5 },
  0b10110000: { [mode]: celtOnly, [bandwidth]: wideBand, [frameSize]: 10 },
  0b10111000: { [mode]: celtOnly, [bandwidth]: wideBand, [frameSize]: 20 },
  0b11000000: { [mode]: celtOnly, [bandwidth]: superWideBand, [frameSize]: 2.5 },
  0b11001000: { [mode]: celtOnly, [bandwidth]: superWideBand, [frameSize]: 5 },
  0b11010000: { [mode]: celtOnly, [bandwidth]: superWideBand, [frameSize]: 10 },
  0b11011000: { [mode]: celtOnly, [bandwidth]: superWideBand, [frameSize]: 20 },
  0b11100000: { [mode]: celtOnly, [bandwidth]: fullBand, [frameSize]: 2.5 },
  0b11101000: { [mode]: celtOnly, [bandwidth]: fullBand, [frameSize]: 5 },
  0b11110000: { [mode]: celtOnly, [bandwidth]: fullBand, [frameSize]: 10 },
  0b11111000: { [mode]: celtOnly, [bandwidth]: fullBand, [frameSize]: 20 },
};

export default class OpusHeader extends CodecHeader {
  static [getHeaderFromUint8Array](dataValue, packetData, headerCache) {
    const header = {};

    // get length of header
    // Byte (10 of 19)
    // * `CCCCCCCC`: Channel Count
    header[channels] = dataValue[9];
    // Byte (19 of 19)
    // * `GGGGGGGG`: Channel Mapping Family
    header[channelMappingFamily] = dataValue[18];

    header[length] =
      header[channelMappingFamily] !== 0 ? 21 + header[channels] : 19;

    if (dataValue[length] < header[length])
      throw new Error("Out of data while inside an Ogg Page");

    // Page Segment Bytes (1-2)
    // * `AAAAA...`: Packet config
    // * `.....B..`:
    // * `......CC`: Packet code
    const packetMode = packetData[0] & 0b00000011;
    const packetLength = packetMode === 3 ? 2 : 1;

    // Check header cache
    const key =
      bytesToString(dataValue[subarray](0, header[length])) +
      bytesToString(packetData[subarray](0, packetLength));
    const cachedHeader = headerCache[getHeader](key);

    if (cachedHeader) return new OpusHeader(cachedHeader);

    // Bytes (1-8 of 19): OpusHead - Magic Signature
    if (key.substr(0, 8) !== "OpusHead") {
      return null;
    }

    // Byte (9 of 19)
    // * `00000001`: Version number
    if (dataValue[8] !== 1) return null;

    header[data] = uint8Array.from(dataValue[subarray](0, header[length]));

    const view = new dataView(header[data][buffer]);

    header[bitDepth] = 16;

    // Byte (10 of 19)
    // * `CCCCCCCC`: Channel Count
    // set earlier to determine length

    // Byte (11-12 of 19)
    // * `DDDDDDDD|DDDDDDDD`: Pre skip
    header[preSkip] = view.getUint16(10, true);

    // Byte (13-16 of 19)
    // * `EEEEEEEE|EEEEEEEE|EEEEEEEE|EEEEEEEE`: Sample Rate
    header[inputSampleRate] = view.getUint32(12, true);
    // Opus is always decoded at 48kHz
    header[sampleRate] = rate48000;

    // Byte (17-18 of 19)
    // * `FFFFFFFF|FFFFFFFF`: Output Gain
    header[outputGain] = view.getInt16(16, true);

    // Byte (19 of 19)
    // * `GGGGGGGG`: Channel Mapping Family
    // set earlier to determine length
    if (header[channelMappingFamily] in channelMappingFamilies) {
      header[channelMode] =
        channelMappingFamilies[header[channelMappingFamily]][
          header[channels] - 1
        ];
      if (!header[channelMode]) return null;
    }

    if (header[channelMappingFamily] !== 0) {
      // * `HHHHHHHH`: Stream count
      header[streamCount] = dataValue[19];

      // * `IIIIIIII`: Coupled Stream count
      header[coupledStreamCount] = dataValue[20];

      // * `JJJJJJJJ|...` Channel Mapping table
      header[channelMappingTable] = [
        ...dataValue[subarray](21, header[channels] + 21),
      ];
    }

    const packetConfig = configTable[0b11111000 & packetData[0]];
    header[mode] = packetConfig[mode];
    header[bandwidth] = packetConfig[bandwidth];
    header[frameSize] = packetConfig[frameSize];

    // https://tools.ietf.org/html/rfc6716#appendix-B
    switch (packetMode) {
      case 0:
        // 0: 1 frame in the packet
        header[frameCount] = 1;
        break;
      case 1:
      // 1: 2 frames in the packet, each with equal compressed size
      case 2:
        // 2: 2 frames in the packet, with different compressed sizes
        header[frameCount] = 2;
        break;
      case 3:
        // 3: an arbitrary number of frames in the packet
        header[isVbr] = !!(0b10000000 & packetData[1]);
        header[hasOpusPadding] = !!(0b01000000 & packetData[1]);
        header[frameCount] = 0b00111111 & packetData[1];
        break;
      default:
        return null;
    }

    // set header cache
    {
      const {
        length,
        data: headerData,
        channelMappingFamily,
        ...codecUpdateFields
      } = header;

      headerCache[setHeader](key, header, codecUpdateFields);
    }

    return new OpusHeader(header);
  }

  /**
   * @private
   * Call OpusHeader.getHeader(Array<Uint8>) to get instance
   */
  constructor(header) {
    super(header);

    this[data] = header[data];
    this[bandwidth] = header[bandwidth];
    this[channelMappingFamily] = header[channelMappingFamily];
    this[channelMappingTable] = header[channelMappingTable];
    this[coupledStreamCount] = header[coupledStreamCount];
    this[frameCount] = header[frameCount];
    this[frameSize] = header[frameSize];
    this[hasOpusPadding] = header[hasOpusPadding];
    this[inputSampleRate] = header[inputSampleRate];
    this[isVbr] = header[isVbr];
    this[mode] = header[mode];
    this[outputGain] = header[outputGain];
    this[preSkip] = header[preSkip];
    this[streamCount] = header[streamCount];
  }
}
