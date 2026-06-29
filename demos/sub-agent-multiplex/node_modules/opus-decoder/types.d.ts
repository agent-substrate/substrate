import { DecodeError } from "@wasm-audio-decoders/common";

export type OpusDecoderDefaultSampleRate = 48000;
export type OpusDecoderSampleRate =
  | 8000
  | 12000
  | 16000
  | 24000
  | OpusDecoderDefaultSampleRate;

export interface OpusDecodedAudio<
  SampleRate extends OpusDecoderSampleRate = OpusDecoderDefaultSampleRate,
> {
  channelData: Float32Array[];
  samplesDecoded: number;
  sampleRate: SampleRate;
  errors: DecodeError[];
}

export class OpusDecoder<
  SampleRate extends OpusDecoderSampleRate | undefined = undefined,
> {
  constructor(options?: {
    forceStereo?: boolean;
    sampleRate?: SampleRate;
    preSkip?: number;
    channels?: number;
    streamCount?: number;
    coupledStreamCount?: number;
    channelMappingTable?: number[];
  });
  ready: Promise<void>;
  reset: () => Promise<void>;
  free: () => void;
  decodeFrame: (
    opusFrame: Uint8Array,
  ) => OpusDecodedAudio<
    SampleRate extends undefined ? OpusDecoderDefaultSampleRate : SampleRate
  >;
  decodeFrames: (
    opusFrames: Uint8Array[],
  ) => OpusDecodedAudio<
    SampleRate extends undefined ? OpusDecoderDefaultSampleRate : SampleRate
  >;
}

export class OpusDecoderWebWorker<
  SampleRate extends OpusDecoderSampleRate | undefined = undefined,
> {
  constructor(options?: {
    forceStereo?: boolean;
    sampleRate?: SampleRate;
    preSkip?: number;
    channels?: number;
    streamCount?: number;
    coupledStreamCount?: number;
    channelMappingTable?: number[];
  });
  ready: Promise<void>;
  reset: () => Promise<void>;
  free: () => Promise<void>;
  decodeFrame: (
    opusFrame: Uint8Array,
  ) => Promise<
    OpusDecodedAudio<
      SampleRate extends undefined ? OpusDecoderDefaultSampleRate : SampleRate
    >
  >;
  decodeFrames: (
    opusFrames: Uint8Array[],
  ) => Promise<
    OpusDecodedAudio<
      SampleRate extends undefined ? OpusDecoderDefaultSampleRate : SampleRate
    >
  >;
}

export { DecodeError };
