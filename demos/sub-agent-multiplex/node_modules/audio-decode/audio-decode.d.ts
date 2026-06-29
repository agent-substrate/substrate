// audio-decode.d.ts

export default function audioDecode(buf: ArrayBuffer | Uint8Array): Promise<AudioBuffer>;

export interface Decoders {
  oga: (buf: Uint8Array) => Promise<AudioBuffer>;
  mp3: (buf: Uint8Array) => Promise<AudioBuffer>;
  flac: (buf: Uint8Array) => Promise<AudioBuffer>;
  opus: (buf: Uint8Array) => Promise<AudioBuffer>;
  wav: (buf: Uint8Array) => Promise<AudioBuffer>;
  qoa: (buf: Uint8Array) => Promise<AudioBuffer>;
}

export const decoders: Decoders;
