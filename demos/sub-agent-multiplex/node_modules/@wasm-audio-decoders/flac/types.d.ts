import { DecodeError } from "@wasm-audio-decoders/common";

export interface FLACDecodedAudio {
  channelData: Float32Array[];
  samplesDecoded: number;
  sampleRate: number;
  bitDepth: number;
  errors: DecodeError[];
}

export class FLACDecoder {
  ready: Promise<void>;
  reset: () => Promise<void>;
  free: () => void;
  decode: (flacData: Uint8Array) => Promise<FLACDecodedAudio>;
  flush: () => Promise<FLACDecodedAudio>;
  decodeFile: (flacData: Uint8Array) => Promise<FLACDecodedAudio>;
  decodeFrames: (flacFrames: Uint8Array[]) => Promise<FLACDecodedAudio>;
}

export class FLACDecoderWebWorker {
  ready: Promise<void>;
  reset: () => Promise<void>;
  free: () => Promise<void>;
  decode: (flacData: Uint8Array) => Promise<FLACDecodedAudio>;
  flush: () => Promise<FLACDecodedAudio>;
  decodeFile: (flacData: Uint8Array) => Promise<FLACDecodedAudio>;
  decodeFrames: (flacFrames: Uint8Array[]) => Promise<FLACDecodedAudio>;
}

export { DecodeError };
