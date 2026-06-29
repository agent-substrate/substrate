import OggOpusDecoder from "./src/OggOpusDecoder.js";
import OggOpusDecoderWebWorker from "./src/OggOpusDecoderWebWorker.js";
import { assignNames } from "@wasm-audio-decoders/common";

assignNames(OggOpusDecoder, "OggOpusDecoder");
assignNames(OggOpusDecoderWebWorker, "OggOpusDecoderWebWorker");

export { OggOpusDecoder, OggOpusDecoderWebWorker };
