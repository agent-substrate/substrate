import { DecodeError } from "@wasm-audio-decoders/common";

export type OpusMLDecoderDefaultSampleRate = 48000;
export type OpusMLDecoderSampleRate =
  | 8000
  | 12000
  | 16000
  | 24000
  | OpusMLDecoderDefaultSampleRate;
export type OpusMLSpeechQualityEnhancementOption = "none" | "lace" | "nolace";

export interface OpusMLDecodedAudio<
  SampleRate extends OpusMLDecoderSampleRate = OpusMLDecoderDefaultSampleRate,
> {
  channelData: Float32Array[];
  samplesDecoded: number;
  sampleRate: SampleRate;
  errors: DecodeError[];
}

export class OpusMLDecoder<
  SampleRate extends OpusMLDecoderSampleRate | undefined = undefined,
> {
  constructor(options?: {
    forceStereo?: boolean;
    speechQualityEnhancement?: OpusMLSpeechQualityEnhancementOption;
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
  ) => OpusMLDecodedAudio<
    SampleRate extends undefined ? OpusMLDecoderDefaultSampleRate : SampleRate
  >;
  decodeFrames: (
    opusFrames: Uint8Array[],
  ) => OpusMLDecodedAudio<
    SampleRate extends undefined ? OpusMLDecoderDefaultSampleRate : SampleRate
  >;
}

export class OpusMLDecoderWebWorker<
  SampleRate extends OpusMLDecoderSampleRate | undefined = undefined,
> {
  constructor(options?: {
    forceStereo?: boolean;
    speechQualityEnhancement?: OpusMLSpeechQualityEnhancementOption;
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
    OpusMLDecodedAudio<
      SampleRate extends undefined ? OpusMLDecoderDefaultSampleRate : SampleRate
    >
  >;
  decodeFrames: (
    opusFrames: Uint8Array[],
  ) => Promise<
    OpusMLDecodedAudio<
      SampleRate extends undefined ? OpusMLDecoderDefaultSampleRate : SampleRate
    >
  >;
}

export { DecodeError };
