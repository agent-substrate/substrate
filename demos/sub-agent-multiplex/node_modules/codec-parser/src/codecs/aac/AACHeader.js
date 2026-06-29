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
https://wiki.multimedia.cx/index.php/ADTS

AAAAAAAA AAAABCCD EEFFFFGH HHIJKLMM MMMMMMMM MMMOOOOO OOOOOOPP (QQQQQQQQ QQQQQQQQ)

AACHeader consists of 7 or 9 bytes (without or with CRC).
Letter  Length (bits)  Description
A  12  syncword 0xFFF, all bits must be 1
B  1   MPEG Version: 0 for MPEG-4, 1 for MPEG-2
C  2   Layer: always 0
D  1   protection absent, Warning, set to 1 if there is no CRC and 0 if there is CRC
E  2   profile, the MPEG-4 Audio Object Type minus 1
F  4   MPEG-4 Sampling Frequency Index (15 is forbidden)
G  1   private bit, guaranteed never to be used by MPEG, set to 0 when encoding, ignore when decoding
H  3   MPEG-4 Channel Configuration (in the case of 0, the channel configuration is sent via an inband PCE)
I  1   originality, set to 0 when encoding, ignore when decoding
J  1   home, set to 0 when encoding, ignore when decoding
K  1   copyrighted id bit, the next bit of a centrally registered copyright identifier, set to 0 when encoding, ignore when decoding
L  1   copyright id start, signals that this frame's copyright id bit is the first bit of the copyright id, set to 0 when encoding, ignore when decoding
M  13  frame length, this value must include 7 or 9 bytes of header length: FrameLength = (ProtectionAbsent == 1 ? 7 : 9) + size(AACFrame)
O  11  Buffer fullness // 0x7FF for VBR
P  2   Number of AAC frames (RDBs) in ADTS frame minus 1, for maximum compatibility always use 1 AAC frame per ADTS frame
Q  16  CRC if protection absent is 0 
*/

import { headerStore } from "../../globals.js";
import { bytesToString } from "../../utilities.js";
import {
  reserved,
  bad,
  none,
  sixteenBitCRC,
  rate96000,
  rate88200,
  rate64000,
  rate48000,
  rate44100,
  rate32000,
  rate24000,
  rate22050,
  rate16000,
  rate12000,
  rate11025,
  rate8000,
  rate7350,
  channelMappings,
  getChannelMapping,
  monophonic,
  lfe,
  bitDepth,
  channelMode,
  sampleRate,
  channels,
  copyrightId,
  copyrightIdStart,
  bufferFullness,
  isHome,
  isOriginal,
  isPrivate,
  layer,
  length,
  mpegVersion,
  numberAACFrames,
  profile,
  protection,
  frameLength,
  description,
  samples,
  sampleRateBits,
  profileBits,
  channelModeBits,
  buffer,
  readRawData,
  getHeader,
  setHeader,
  uint8Array,
  dataView,
} from "../../constants.js";

import CodecHeader from "../CodecHeader.js";

const mpegVersionValues = {
  0b00000000: "MPEG-4",
  0b00001000: "MPEG-2",
};

const layerValues = {
  0b00000000: "valid",
  0b00000010: bad,
  0b00000100: bad,
  0b00000110: bad,
};

const protectionValues = {
  0b00000000: sixteenBitCRC,
  0b00000001: none,
};

const profileValues = {
  0b00000000: "AAC Main",
  0b01000000: "AAC LC (Low Complexity)",
  0b10000000: "AAC SSR (Scalable Sample Rate)",
  0b11000000: "AAC LTP (Long Term Prediction)",
};

const sampleRates = {
  0b00000000: rate96000,
  0b00000100: rate88200,
  0b00001000: rate64000,
  0b00001100: rate48000,
  0b00010000: rate44100,
  0b00010100: rate32000,
  0b00011000: rate24000,
  0b00011100: rate22050,
  0b00100000: rate16000,
  0b00100100: rate12000,
  0b00101000: rate11025,
  0b00101100: rate8000,
  0b00110000: rate7350,
  0b00110100: reserved,
  0b00111000: reserved,
  0b00111100: "frequency is written explicitly",
};

// prettier-ignore
const channelModeValues = {
  0b000000000: { [channels]: 0, [description]: "Defined in AOT Specific Config" },
  /*
  'monophonic (mono)'
  'stereo (left, right)'
  'linear surround (front center, front left, front right)'
  'quadraphonic (front center, front left, front right, rear center)'
  '5.0 surround (front center, front left, front right, rear left, rear right)'
  '5.1 surround (front center, front left, front right, rear left, rear right, LFE)'
  '7.1 surround (front center, front left, front right, side left, side right, rear left, rear right, LFE)'
  */
  0b001000000: { [channels]: 1, [description]: monophonic },
  0b010000000: { [channels]: 2, [description]: getChannelMapping(2,channelMappings[0][0]) },
  0b011000000: { [channels]: 3, [description]: getChannelMapping(3,channelMappings[1][3]), },
  0b100000000: { [channels]: 4, [description]: getChannelMapping(4,channelMappings[1][3],channelMappings[3][4]), },
  0b101000000: { [channels]: 5, [description]: getChannelMapping(5,channelMappings[1][3],channelMappings[3][0]), },
  0b110000000: { [channels]: 6, [description]: getChannelMapping(6,channelMappings[1][3],channelMappings[3][0],lfe), },
  0b111000000: { [channels]: 8, [description]: getChannelMapping(8,channelMappings[1][3],channelMappings[2][0],channelMappings[3][0],lfe), },
};

export default class AACHeader extends CodecHeader {
  static *[getHeader](codecParser, headerCache, readOffset) {
    const header = {};

    // Must be at least seven bytes. Out of data
    const data = yield* codecParser[readRawData](7, readOffset);

    // Check header cache
    const key = bytesToString([
      data[0],
      data[1],
      data[2],
      (data[3] & 0b11111100) | (data[6] & 0b00000011), // frame length, buffer fullness varies so don't cache it
    ]);
    const cachedHeader = headerCache[getHeader](key);

    if (!cachedHeader) {
      // Frame sync (all bits must be set): `11111111|1111`:
      if (data[0] !== 0xff || data[1] < 0xf0) return null;

      // Byte (2 of 7)
      // * `1111BCCD`
      // * `....B...`: MPEG Version: 0 for MPEG-4, 1 for MPEG-2
      // * `.....CC.`: Layer: always 0
      // * `.......D`: protection absent, Warning, set to 1 if there is no CRC and 0 if there is CRC
      header[mpegVersion] = mpegVersionValues[data[1] & 0b00001000];

      header[layer] = layerValues[data[1] & 0b00000110];
      if (header[layer] === bad) return null;

      const protectionBit = data[1] & 0b00000001;
      header[protection] = protectionValues[protectionBit];
      header[length] = protectionBit ? 7 : 9;

      // Byte (3 of 7)
      // * `EEFFFFGH`
      // * `EE......`: profile, the MPEG-4 Audio Object Type minus 1
      // * `..FFFF..`: MPEG-4 Sampling Frequency Index (15 is forbidden)
      // * `......G.`: private bit, guaranteed never to be used by MPEG, set to 0 when encoding, ignore when decoding
      header[profileBits] = data[2] & 0b11000000;
      header[sampleRateBits] = data[2] & 0b00111100;
      const privateBit = data[2] & 0b00000010;

      header[profile] = profileValues[header[profileBits]];

      header[sampleRate] = sampleRates[header[sampleRateBits]];
      if (header[sampleRate] === reserved) return null;

      header[isPrivate] = !!privateBit;

      // Byte (3,4 of 7)
      // * `.......H|HH......`: MPEG-4 Channel Configuration (in the case of 0, the channel configuration is sent via an inband PCE)
      header[channelModeBits] = ((data[2] << 8) | data[3]) & 0b111000000;
      header[channelMode] =
        channelModeValues[header[channelModeBits]][description];
      header[channels] = channelModeValues[header[channelModeBits]][channels];

      // Byte (4 of 7)
      // * `HHIJKLMM`
      // * `..I.....`: originality, set to 0 when encoding, ignore when decoding
      // * `...J....`: home, set to 0 when encoding, ignore when decoding
      // * `....K...`: copyrighted id bit, the next bit of a centrally registered copyright identifier, set to 0 when encoding, ignore when decoding
      // * `.....L..`: copyright id start, signals that this frame's copyright id bit is the first bit of the copyright id, set to 0 when encoding, ignore when decoding
      header[isOriginal] = !!(data[3] & 0b00100000);
      header[isHome] = !!(data[3] & 0b00001000);
      header[copyrightId] = !!(data[3] & 0b00001000);
      header[copyrightIdStart] = !!(data[3] & 0b00000100);
      header[bitDepth] = 16;
      header[samples] = 1024;

      // Byte (7 of 7)
      // * `......PP` Number of AAC frames (RDBs) in ADTS frame minus 1, for maximum compatibility always use 1 AAC frame per ADTS frame
      header[numberAACFrames] = data[6] & 0b00000011;

      {
        const {
          length,
          channelModeBits,
          profileBits,
          sampleRateBits,
          frameLength,
          samples,
          numberAACFrames,
          ...codecUpdateFields
        } = header;
        headerCache[setHeader](key, header, codecUpdateFields);
      }
    } else {
      Object.assign(header, cachedHeader);
    }

    // Byte (4,5,6 of 7)
    // * `.......MM|MMMMMMMM|MMM.....`: frame length, this value must include 7 or 9 bytes of header length: FrameLength = (ProtectionAbsent == 1 ? 7 : 9) + size(AACFrame)
    header[frameLength] =
      ((data[3] << 11) | (data[4] << 3) | (data[5] >> 5)) & 0x1fff;
    if (!header[frameLength]) return null;

    // Byte (6,7 of 7)
    // * `...OOOOO|OOOOOO..`: Buffer fullness
    const bufferFullnessBits = ((data[5] << 6) | (data[6] >> 2)) & 0x7ff;
    header[bufferFullness] =
      bufferFullnessBits === 0x7ff ? "VBR" : bufferFullnessBits;

    return new AACHeader(header);
  }

  /**
   * @private
   * Call AACHeader.getHeader(Array<Uint8>) to get instance
   */
  constructor(header) {
    super(header);

    this[copyrightId] = header[copyrightId];
    this[copyrightIdStart] = header[copyrightIdStart];
    this[bufferFullness] = header[bufferFullness];
    this[isHome] = header[isHome];
    this[isOriginal] = header[isOriginal];
    this[isPrivate] = header[isPrivate];
    this[layer] = header[layer];
    this[length] = header[length];
    this[mpegVersion] = header[mpegVersion];
    this[numberAACFrames] = header[numberAACFrames];
    this[profile] = header[profile];
    this[protection] = header[protection];
  }

  get audioSpecificConfig() {
    // Audio Specific Configuration
    // * `000EEFFF|F0HHH000`:
    // * `000EE...|........`: Object Type (profileBit + 1)
    // * `.....FFF|F.......`: Sample Rate
    // * `........|.0HHH...`: Channel Configuration
    // * `........|.....0..`: Frame Length (1024)
    // * `........|......0.`: does not depend on core coder
    // * `........|.......0`: Not Extension
    const header = headerStore.get(this);

    const audioSpecificConfig =
      ((header[profileBits] + 0x40) << 5) |
      (header[sampleRateBits] << 5) |
      (header[channelModeBits] >> 3);

    const bytes = new uint8Array(2);
    new dataView(bytes[buffer]).setUint16(0, audioSpecificConfig, false);
    return bytes;
  }
}
