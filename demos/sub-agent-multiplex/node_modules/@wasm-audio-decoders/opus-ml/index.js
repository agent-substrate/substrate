import OpusMLDecoder from "./src/OpusMLDecoder.js";
import OpusMLDecoderWebWorker from "./src/OpusMLDecoderWebWorker.js";
import { assignNames } from "@wasm-audio-decoders/common";

assignNames(OpusMLDecoder, "OpusMLDecoder");
assignNames(OpusMLDecoderWebWorker, "OpusMLDecoderWebWorker");

export { OpusMLDecoder, OpusMLDecoderWebWorker };
