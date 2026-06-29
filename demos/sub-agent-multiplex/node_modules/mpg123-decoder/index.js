import MPEGDecoder from "./src/MPEGDecoder.js";
import MPEGDecoderWebWorker from "./src/MPEGDecoderWebWorker.js";
import { assignNames } from "@wasm-audio-decoders/common";

assignNames(MPEGDecoder, "MPEGDecoder");
assignNames(MPEGDecoderWebWorker, "MPEGDecoderWebWorker");

export { MPEGDecoder, MPEGDecoderWebWorker };
