import OpusDecoder from "./src/OpusDecoder.js";
import OpusDecoderWebWorker from "./src/OpusDecoderWebWorker.js";
import { assignNames } from "@wasm-audio-decoders/common";

assignNames(OpusDecoder, "OpusDecoder");
assignNames(OpusDecoderWebWorker, "OpusDecoderWebWorker");

export { OpusDecoder, OpusDecoderWebWorker };
