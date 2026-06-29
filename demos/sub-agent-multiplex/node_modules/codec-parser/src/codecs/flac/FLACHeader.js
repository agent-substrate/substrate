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
https://xiph.org/flac/format.html

AAAAAAAA AAAAAABC DDDDEEEE FFFFGGGH 
(IIIIIIII...)
(JJJJJJJJ|JJJJJJJJ)
(KKKKKKKK|KKKKKKKK)
LLLLLLLLL

FLAC Frame Header
Letter  Length (bits)  Description
A   13  11111111|11111
B   1   Reserved 0 - mandatory, 1 - reserved
C   1   Blocking strategy, 0 - fixed, 1 - variable
D   4   Block size in inter-channel samples
E   4   Sample rate
F   4   Channel assignment
G   3   Sample size in bits
H   1   Reserved 0 - mandatory, 1 - reserved
I   ?   if(variable blocksize)
           <8-56>:"UTF-8" coded sample number (decoded number is 36 bits) [4]
        else
           <8-48>:"UTF-8" coded frame number (decoded number is 31 bits) [4]
J   ?   if(blocksize bits == 011x)
            8/16 bit (blocksize-1)
K   ?   if(sample rate bits == 11xx)
            8/16 bit sample rate
L   8   CRC-8 (polynomial = x^8 + x^2 + x^1 + x^0, initialized with 0) of everything before the crc, including the sync code
        
*/

import {
  reserved,
  bad,
  rate88200,
  rate176400,
  rate192000,
  rate8000,
  rate16000,
  rate22050,
  rate24000,
  rate32000,
  rate44100,
  rate48000,
  rate96000,
  channelMappings,
  getChannelMapping,
  monophonic,
  stereo,
  lfe,
  bitDepth,
  channelMode,
  sampleRate,
  channels,
  length,
  crc16,
  blockingStrategy,
  blockSize,
  frameNumber,
  sampleNumber,
  streamInfo,
  description,
  samples,
  sampleRateBits,
  blockingStrategyBits,
  blockSizeBits,
  crc,
  subarray,
  readRawData,
  getHeader,
  setHeader,
  getHeaderFromUint8Array,
} from "../../constants.js";
import { bytesToString, crc8 } from "../../utilities.js";
import CodecHeader from "../CodecHeader.js";

const getFromStreamInfo = "get from STREAMINFO metadata block";

const blockingStrategyValues = {
  0b00000000: "Fixed",
  0b00000001: "Variable",
};

const blockSizeValues = {
  0b00000000: reserved,
  0b00010000: 192,
  // 0b00100000: 576,
  // 0b00110000: 1152,
  // 0b01000000: 2304,
  // 0b01010000: 4608,
  // 0b01100000: "8-bit (blocksize-1) from end of header",
  // 0b01110000: "16-bit (blocksize-1) from end of header",
  // 0b10000000: 256,
  // 0b10010000: 512,
  // 0b10100000: 1024,
  // 0b10110000: 2048,
  // 0b11000000: 4096,
  // 0b11010000: 8192,
  // 0b11100000: 16384,
  // 0b11110000: 32768,
};
for (let i = 2; i < 16; i++)
  blockSizeValues[i << 4] = i < 6 ? 576 * 2 ** (i - 2) : 2 ** i;

const sampleRateValues = {
  0b00000000: getFromStreamInfo,
  0b00000001: rate88200,
  0b00000010: rate176400,
  0b00000011: rate192000,
  0b00000100: rate8000,
  0b00000101: rate16000,
  0b00000110: rate22050,
  0b00000111: rate24000,
  0b00001000: rate32000,
  0b00001001: rate44100,
  0b00001010: rate48000,
  0b00001011: rate96000,
  // 0b00001100: "8-bit sample rate (in kHz) from end of header",
  // 0b00001101: "16-bit sample rate (in Hz) from end of header",
  // 0b00001110: "16-bit sample rate (in tens of Hz) from end of header",
  0b00001111: bad,
};

/* prettier-ignore */
const channelAssignments = {
  /*'
  'monophonic (mono)'
  'stereo (left, right)'
  'linear surround (left, right, center)'
  'quadraphonic (front left, front right, rear left, rear right)'
  '5.0 surround (front left, front right, front center, rear left, rear right)'
  '5.1 surround (front left, front right, front center, LFE, rear left, rear right)'
  '6.1 surround (front left, front right, front center, LFE, rear center, side left, side right)'
  '7.1 surround (front left, front right, front center, LFE, rear left, rear right, side left, side right)'
  */
  0b00000000: {[channels]: 1, [description]: monophonic},
  0b00010000: {[channels]: 2, [description]: getChannelMapping(2,channelMappings[0][0])},
  0b00100000: {[channels]: 3, [description]: getChannelMapping(3,channelMappings[0][1])},
  0b00110000: {[channels]: 4, [description]: getChannelMapping(4,channelMappings[1][0],channelMappings[3][0])},
  0b01000000: {[channels]: 5, [description]: getChannelMapping(5,channelMappings[1][1],channelMappings[3][0])},
  0b01010000: {[channels]: 6, [description]: getChannelMapping(6,channelMappings[1][1],lfe,channelMappings[3][0])},
  0b01100000: {[channels]: 7, [description]: getChannelMapping(7,channelMappings[1][1],lfe,channelMappings[3][4],channelMappings[2][0])},
  0b01110000: {[channels]: 8, [description]: getChannelMapping(8,channelMappings[1][1],lfe,channelMappings[3][0],channelMappings[2][0])},
  0b10000000: {[channels]: 2, [description]: `${stereo} (left, diff)`},
  0b10010000: {[channels]: 2, [description]: `${stereo} (diff, right)`},
  0b10100000: {[channels]: 2, [description]: `${stereo} (avg, diff)`},
  0b10110000: reserved,
  0b11000000: reserved,
  0b11010000: reserved,
  0b11100000: reserved,
  0b11110000: reserved,
}

const bitDepthValues = {
  0b00000000: getFromStreamInfo,
  0b00000010: 8,
  0b00000100: 12,
  0b00000110: reserved,
  0b00001000: 16,
  0b00001010: 20,
  0b00001100: 24,
  0b00001110: reserved,
};

export default class FLACHeader extends CodecHeader {
  // https://datatracker.ietf.org/doc/html/rfc3629#section-3
  //    Char. number range  |        UTF-8 octet sequence
  //    (hexadecimal)    |              (binary)
  // --------------------+---------------------------------------------
  // 0000 0000-0000 007F | 0xxxxxxx
  // 0000 0080-0000 07FF | 110xxxxx 10xxxxxx
  // 0000 0800-0000 FFFF | 1110xxxx 10xxxxxx 10xxxxxx
  // 0001 0000-0010 FFFF | 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
  static _decodeUTF8Int(data) {
    if (data[0] > 0xfe) {
      return null; // length byte must have at least one zero as the lsb
    }

    if (data[0] < 0x80) return { value: data[0], length: 1 };

    // get length by counting the number of msb that are set to 1
    let length = 1;
    for (let zeroMask = 0x40; zeroMask & data[0]; zeroMask >>= 1) length++;

    let idx = length - 1,
      value = 0,
      shift = 0;

    // sum together the encoded bits in bytes 2 to length
    // 1110xxxx 10[cccccc] 10[bbbbbb] 10[aaaaaa]
    //
    //    value = [cccccc] | [bbbbbb] | [aaaaaa]
    for (; idx > 0; shift += 6, idx--) {
      if ((data[idx] & 0xc0) !== 0x80) {
        return null; // each byte should have leading 10xxxxxx
      }
      value |= (data[idx] & 0x3f) << shift; // add the encoded bits
    }

    // read the final encoded bits in byte 1
    //     1110[dddd] 10[cccccc] 10[bbbbbb] 10[aaaaaa]
    //
    // value = [dddd] | [cccccc] | [bbbbbb] | [aaaaaa]
    value |= (data[idx] & (0x7f >> length)) << shift;

    return { value, length };
  }

  static [getHeaderFromUint8Array](data, headerCache) {
    const codecParserStub = {
      [readRawData]: function* () {
        return data;
      },
    };

    return FLACHeader[getHeader](codecParserStub, headerCache, 0).next().value;
  }

  static *[getHeader](codecParser, headerCache, readOffset) {
    // Must be at least 6 bytes.
    let data = yield* codecParser[readRawData](6, readOffset);

    // Bytes (1-2 of 6)
    // * `11111111|111110..`: Frame sync
    // * `........|......0.`: Reserved 0 - mandatory, 1 - reserved
    if (data[0] !== 0xff || !(data[1] === 0xf8 || data[1] === 0xf9)) {
      return null;
    }

    const header = {};

    // Check header cache
    const key = bytesToString(data[subarray](0, 4));
    const cachedHeader = headerCache[getHeader](key);

    if (!cachedHeader) {
      // Byte (2 of 6)
      // * `.......C`: Blocking strategy, 0 - fixed, 1 - variable
      header[blockingStrategyBits] = data[1] & 0b00000001;
      header[blockingStrategy] =
        blockingStrategyValues[header[blockingStrategyBits]];

      // Byte (3 of 6)
      // * `DDDD....`: Block size in inter-channel samples
      // * `....EEEE`: Sample rate
      header[blockSizeBits] = data[2] & 0b11110000;
      header[sampleRateBits] = data[2] & 0b00001111;

      header[blockSize] = blockSizeValues[header[blockSizeBits]];
      if (header[blockSize] === reserved) {
        return null;
      }

      header[sampleRate] = sampleRateValues[header[sampleRateBits]];
      if (header[sampleRate] === bad) {
        return null;
      }

      // Byte (4 of 6)
      // * `FFFF....`: Channel assignment
      // * `....GGG.`: Sample size in bits
      // * `.......H`: Reserved 0 - mandatory, 1 - reserved
      if (data[3] & 0b00000001) {
        return null;
      }

      const channelAssignment = channelAssignments[data[3] & 0b11110000];
      if (channelAssignment === reserved) {
        return null;
      }

      header[channels] = channelAssignment[channels];
      header[channelMode] = channelAssignment[description];

      header[bitDepth] = bitDepthValues[data[3] & 0b00001110];
      if (header[bitDepth] === reserved) {
        return null;
      }
    } else {
      Object.assign(header, cachedHeader);
    }

    // Byte (5...)
    // * `IIIIIIII|...`: VBR block size ? sample number : frame number
    header[length] = 5;

    // check if there is enough data to parse UTF8
    data = yield* codecParser[readRawData](header[length] + 8, readOffset);

    const decodedUtf8 = FLACHeader._decodeUTF8Int(data[subarray](4));
    if (!decodedUtf8) {
      return null;
    }

    if (header[blockingStrategyBits]) {
      header[sampleNumber] = decodedUtf8.value;
    } else {
      header[frameNumber] = decodedUtf8.value;
    }

    header[length] += decodedUtf8[length];

    // Byte (...)
    // * `JJJJJJJJ|(JJJJJJJJ)`: Blocksize (8/16bit custom value)
    if (header[blockSizeBits] === 0b01100000) {
      // 8 bit
      if (data[length] < header[length])
        data = yield* codecParser[readRawData](header[length], readOffset);

      header[blockSize] = data[header[length] - 1] + 1;
      header[length] += 1;
    } else if (header[blockSizeBits] === 0b01110000) {
      // 16 bit
      if (data[length] < header[length])
        data = yield* codecParser[readRawData](header[length], readOffset);

      header[blockSize] =
        (data[header[length] - 1] << 8) + data[header[length]] + 1;
      header[length] += 2;
    }

    header[samples] = header[blockSize];

    // Byte (...)
    // * `KKKKKKKK|(KKKKKKKK)`: Sample rate (8/16bit custom value)
    if (header[sampleRateBits] === 0b00001100) {
      // 8 bit
      if (data[length] < header[length])
        data = yield* codecParser[readRawData](header[length], readOffset);

      header[sampleRate] = data[header[length] - 1] * 1000;
      header[length] += 1;
    } else if (header[sampleRateBits] === 0b00001101) {
      // 16 bit
      if (data[length] < header[length])
        data = yield* codecParser[readRawData](header[length], readOffset);

      header[sampleRate] =
        (data[header[length] - 1] << 8) + data[header[length]];
      header[length] += 2;
    } else if (header[sampleRateBits] === 0b00001110) {
      // 16 bit
      if (data[length] < header[length])
        data = yield* codecParser[readRawData](header[length], readOffset);

      header[sampleRate] =
        ((data[header[length] - 1] << 8) + data[header[length]]) * 10;
      header[length] += 2;
    }

    // Byte (...)
    // * `LLLLLLLL`: CRC-8
    if (data[length] < header[length])
      data = yield* codecParser[readRawData](header[length], readOffset);

    header[crc] = data[header[length] - 1];
    if (header[crc] !== crc8(data[subarray](0, header[length] - 1))) {
      return null;
    }

    {
      if (!cachedHeader) {
        const {
          blockingStrategyBits,
          frameNumber,
          sampleNumber,
          samples,
          sampleRateBits,
          blockSizeBits,
          crc,
          length,
          ...codecUpdateFields
        } = header;
        headerCache[setHeader](key, header, codecUpdateFields);
      }
    }
    return new FLACHeader(header);
  }

  /**
   * @private
   * Call FLACHeader.getHeader(Array<Uint8>) to get instance
   */
  constructor(header) {
    super(header);

    this[crc16] = null; // set in FLACFrame
    this[blockingStrategy] = header[blockingStrategy];
    this[blockSize] = header[blockSize];
    this[frameNumber] = header[frameNumber];
    this[sampleNumber] = header[sampleNumber];
    this[streamInfo] = null; // set during ogg parsing
  }
}
