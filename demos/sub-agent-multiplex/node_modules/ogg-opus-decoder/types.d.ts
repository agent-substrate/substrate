import { DecodeError } from "@wasm-audio-decoders/common";
import { OpusMLSpeechQualityEnhancementOption } from "@wasm-audio-decoders/opus-ml";

export interface OggOpusDecodedAudio {
  channelData: Float32Array[];
  samplesDecoded: number;
  sampleRate: 48000;
  errors: DecodeError[];
}

export class OggOpusDecoder {
  public constructor(options?: {
    forceStereo?: boolean;
    speechQualityEnhancement?: OpusMLSpeechQualityEnhancementOption;
  });
  ready: Promise<void>;
  reset: () => Promise<void>;
  free: () => void;
  decode: (data: Uint8Array) => OggOpusDecodedAudio;
  decodeFile: (data: Uint8Array) => Promise<OggOpusDecodedAudio>;
  flush: () => Promise<OggOpusDecodedAudio>;
}

export class OggOpusDecoderWebWorker {
  public constructor(options?: {
    forceStereo?: boolean;
    speechQualityEnhancement?: OpusMLSpeechQualityEnhancementOption;
  });
  ready: Promise<void>;
  reset: () => Promise<void>;
  free: () => Promise<void>;
  decode: (data: Uint8Array) => Promise<OggOpusDecodedAudio>;
  decodeFile: (data: Uint8Array) => Promise<OggOpusDecodedAudio>;
  flush: () => Promise<OggOpusDecodedAudio>;
}

export { DecodeError };
