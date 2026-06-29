import { DecodeError } from "@wasm-audio-decoders/common";

export interface MPEGDecodedAudio {
  channelData: Float32Array[];
  samplesDecoded: number;
  sampleRate: number;
  errors: DecodeError[];
}

export class MPEGDecoder {
  constructor(options?: { enableGapless?: boolean });
  ready: Promise<void>;
  reset: () => Promise<void>;
  free: () => void;
  decode: (mpegData: Uint8Array) => MPEGDecodedAudio;
  decodeFrame: (mpegFrame: Uint8Array) => MPEGDecodedAudio;
  decodeFrames: (mpegFrames: Uint8Array[]) => MPEGDecodedAudio;
}

export class MPEGDecoderWebWorker {
  constructor(options?: { enableGapless?: boolean });
  ready: Promise<void>;
  reset: () => Promise<void>;
  free: () => Promise<void>;
  decode: (mpegData: Uint8Array) => Promise<MPEGDecodedAudio>;
  decodeFrame: (mpegFrame: Uint8Array) => Promise<MPEGDecodedAudio>;
  decodeFrames: (mpegFrames: Uint8Array[]) => Promise<MPEGDecodedAudio>;
}

export { DecodeError };
