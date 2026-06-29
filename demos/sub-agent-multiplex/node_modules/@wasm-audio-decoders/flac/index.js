import FLACDecoder from "./src/FLACDecoder.js";
import FLACDecoderWebWorker from "./src/FLACDecoderWebWorker.js";
import { assignNames } from "@wasm-audio-decoders/common";

assignNames(FLACDecoder, "FLACDecoder");
assignNames(FLACDecoderWebWorker, "FLACDecoderWebWorker");

export { FLACDecoder, FLACDecoderWebWorker };
