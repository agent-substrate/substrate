import OggVorbisDecoder from "./src/OggVorbisDecoder.js";
import OggVorbisDecoderWebWorker from "./src/OggVorbisDecoderWebWorker.js";
import { assignNames } from "@wasm-audio-decoders/common";

assignNames(OggVorbisDecoder, "OggVorbisDecoder");
assignNames(OggVorbisDecoderWebWorker, "OggVorbisDecoderWebWorker");

export { OggVorbisDecoder, OggVorbisDecoderWebWorker };
