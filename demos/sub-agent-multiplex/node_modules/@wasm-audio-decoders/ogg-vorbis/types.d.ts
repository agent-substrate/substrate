import { DecodeError } from "@wasm-audio-decoders/common";
import { OggPage } from "codec-parser";

export interface OggVorbisDecodedAudio {
  channelData: Float32Array[];
  samplesDecoded: number;
  sampleRate: number;
  bitDepth: 16;
  errors: DecodeError[];
}

export class OggVorbisDecoder {
  ready: Promise<void>;
  reset: () => Promise<void>;
  free: () => void;
  decode: (vorbisData: Uint8Array) => Promise<OggVorbisDecodedAudio>;
  flush: () => Promise<OggVorbisDecodedAudio>;
  decodeFile: (vorbisData: Uint8Array) => Promise<OggVorbisDecodedAudio>;
  decodeOggPages: (oggPages: OggPage[]) => Promise<OggVorbisDecodedAudio>;
}

export class OggVorbisDecoderWebWorker {
  ready: Promise<void>;
  reset: () => Promise<void>;
  free: () => Promise<void>;
  decode: (vorbisData: Uint8Array) => Promise<OggVorbisDecodedAudio>;
  flush: () => Promise<OggVorbisDecodedAudio>;
  decodeFile: (vorbisData: Uint8Array) => Promise<OggVorbisDecodedAudio>;
  decodeOggPages: (oggPages: OggPage[]) => Promise<OggVorbisDecodedAudio>;
}

export { DecodeError };
