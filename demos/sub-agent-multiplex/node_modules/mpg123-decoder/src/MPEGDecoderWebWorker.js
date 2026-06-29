import { WASMAudioDecoderWorker } from "@wasm-audio-decoders/common";
import EmscriptenWASM from "./EmscriptenWasm.js";
import MPEGDecoder from "./MPEGDecoder.js";

export default class MPEGDecoderWebWorker extends WASMAudioDecoderWorker {
  constructor(options) {
    super(options, "mpg123-decoder", MPEGDecoder, EmscriptenWASM);
  }

  async decode(data) {
    return this.postToDecoder("decode", data);
  }

  async decodeFrame(data) {
    return this.postToDecoder("decodeFrame", data);
  }

  async decodeFrames(data) {
    return this.postToDecoder("decodeFrames", data);
  }
}
