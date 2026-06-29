const symbol = Symbol;

// prettier-ignore
/*
[
  [
    "left, right",
    "left, right, center",
    "left, center, right",
    "center, left, right",
    "center"
  ],
  [
    "front left, front right",
    "front left, front right, front center",
    "front left, front center, front right",
    "front center, front left, front right",
    "front center"
  ],
  [
    "side left, side right",
    "side left, side right, side center",
    "side left, side center, side right",
    "side center, side left, side right",
    "side center"
  ],
  [
    "rear left, rear right",
    "rear left, rear right, rear center",
    "rear left, rear center, rear right",
    "rear center, rear left, rear right",
    "rear center"
  ]
]
*/

const mappingJoin = ", ";

export const channelMappings = (() => {
  const front = "front";
  const side = "side";
  const rear = "rear";
  const left = "left";
  const center = "center";
  const right = "right";

  return ["", front + " ", side + " ", rear + " "].map((x) =>
    [
      [left, right],
      [left, right, center],
      [left, center, right],
      [center, left, right],
      [center],
    ].flatMap((y) => y.map((z) => x + z).join(mappingJoin)),
  );
})();

export const lfe = "LFE";
export const monophonic = "monophonic (mono)";
export const stereo = "stereo";
const surround = "surround";

export const getChannelMapping = (channelCount, ...mappings) =>
  `${
    [
      monophonic,
      stereo,
      `linear ${surround}`,
      "quadraphonic",
      `5.0 ${surround}`,
      `5.1 ${surround}`,
      `6.1 ${surround}`,
      `7.1 ${surround}`,
    ][channelCount - 1]
  } (${mappings.join(mappingJoin)})`;

// prettier-ignore
export const vorbisOpusChannelMapping = [
  monophonic,
  getChannelMapping(2,channelMappings[0][0]),
  getChannelMapping(3,channelMappings[0][2]),
  getChannelMapping(4,channelMappings[1][0],channelMappings[3][0]),
  getChannelMapping(5,channelMappings[1][2],channelMappings[3][0]),
  getChannelMapping(6,channelMappings[1][2],channelMappings[3][0],lfe),
  getChannelMapping(7,channelMappings[1][2],channelMappings[2][0],channelMappings[3][4],lfe),
  getChannelMapping(8,channelMappings[1][2],channelMappings[2][0],channelMappings[3][0],lfe),
]

// sampleRates
export const rate192000 = 192000;
export const rate176400 = 176400;
export const rate96000 = 96000;
export const rate88200 = 88200;
export const rate64000 = 64000;
export const rate48000 = 48000;
export const rate44100 = 44100;
export const rate32000 = 32000;
export const rate24000 = 24000;
export const rate22050 = 22050;
export const rate16000 = 16000;
export const rate12000 = 12000;
export const rate11025 = 11025;
export const rate8000 = 8000;
export const rate7350 = 7350;

// header key constants
export const absoluteGranulePosition = "absoluteGranulePosition";
export const bandwidth = "bandwidth";
export const bitDepth = "bitDepth";
export const bitrate = "bitrate";
export const bitrateMaximum = bitrate + "Maximum";
export const bitrateMinimum = bitrate + "Minimum";
export const bitrateNominal = bitrate + "Nominal";
export const buffer = "buffer";
export const bufferFullness = buffer + "Fullness";
export const codec = "codec";
export const codecFrames = codec + "Frames";
export const coupledStreamCount = "coupledStreamCount";
export const crc = "crc";
export const crc16 = crc + "16";
export const crc32 = crc + "32";
export const data = "data";
export const description = "description";
export const duration = "duration";
export const emphasis = "emphasis";
export const hasOpusPadding = "hasOpusPadding";
export const header = "header";
export const isContinuedPacket = "isContinuedPacket";
export const isCopyrighted = "isCopyrighted";
export const isFirstPage = "isFirstPage";
export const isHome = "isHome";
export const isLastPage = "isLastPage";
export const isOriginal = "isOriginal";
export const isPrivate = "isPrivate";
export const isVbr = "isVbr";
export const layer = "layer";
export const length = "length";
export const mode = "mode";
export const modeExtension = mode + "Extension";
export const mpeg = "mpeg";
export const mpegVersion = mpeg + "Version";
export const numberAACFrames = "numberAAC" + "Frames";
export const outputGain = "outputGain";
export const preSkip = "preSkip";
export const profile = "profile";
export const profileBits = symbol();
export const protection = "protection";
export const rawData = "rawData";
export const segments = "segments";
export const subarray = "subarray";
export const version = "version";
export const vorbis = "vorbis";
export const vorbisComments = vorbis + "Comments";
export const vorbisSetup = vorbis + "Setup";

const block = "block";
export const blockingStrategy = block + "ingStrategy";
export const blockingStrategyBits = symbol();
export const blockSize = block + "Size";
export const blocksize0 = block + "size0";
export const blocksize1 = block + "size1";
export const blockSizeBits = symbol();

const channel = "channel";
export const channelMappingFamily = channel + "MappingFamily";
export const channelMappingTable = channel + "MappingTable";
export const channelMode = channel + "Mode";
export const channelModeBits = symbol();
export const channels = channel + "s";

const copyright = "copyright";
export const copyrightId = copyright + "Id";
export const copyrightIdStart = copyright + "IdStart";

export const frame = "frame";
export const frameCount = frame + "Count";
export const frameLength = frame + "Length";

const Number = "Number";
export const frameNumber = frame + Number;
export const framePadding = frame + "Padding";
export const frameSize = frame + "Size";

const Rate = "Rate";
export const inputSampleRate = "inputSample" + Rate;

const page = "page";
export const pageChecksum = page + "Checksum";
export const pageSegmentBytes = symbol();
export const pageSegmentTable = page + "SegmentTable";
export const pageSequenceNumber = page + "Sequence" + Number;

const sample = "sample";
export const sampleNumber = sample + Number;
export const sampleRate = sample + Rate;
export const sampleRateBits = symbol();
export const samples = sample + "s";

const stream = "stream";
export const streamCount = stream + "Count";
export const streamInfo = stream + "Info";
export const streamSerialNumber = stream + "Serial" + Number;
export const streamStructureVersion = stream + "StructureVersion";

const total = "total";
export const totalBytesOut = total + "BytesOut";
export const totalDuration = total + "Duration";
export const totalSamples = total + "Samples";

// private methods
export const readRawData = symbol();
export const incrementRawData = symbol();
export const mapCodecFrameStats = symbol();
export const mapFrameStats = symbol();
export const logWarning = symbol();
export const logError = symbol();
export const syncFrame = symbol();
export const fixedLengthFrameSync = symbol();
export const getHeader = symbol();
export const setHeader = symbol();
export const getFrame = symbol();
export const parseFrame = symbol();
export const parseOggPage = symbol();
export const checkCodecUpdate = symbol();
export const reset = symbol();
export const enable = symbol();
export const getHeaderFromUint8Array = symbol();
export const checkFrameFooterCrc16 = symbol();

export const uint8Array = Uint8Array;
export const dataView = DataView;

export const reserved = "reserved";
export const bad = "bad";
export const free = "free";
export const none = "none";
export const sixteenBitCRC = "16bit CRC";
