import { WASMAudioDecoderWorker } from "@wasm-audio-decoders/common";
import EmscriptenWASM from "./EmscriptenWasm.js";
import OpusDecoder from "./OpusDecoder.js";

export default class OpusDecoderWebWorker extends WASMAudioDecoderWorker {
  constructor(options) {
    super(options, "opus-decoder", OpusDecoder, EmscriptenWASM);
  }

  async decodeFrame(data) {
    return this.postToDecoder("decodeFrame", data);
  }

  async decodeFrames(data) {
    return this.postToDecoder("decodeFrames", data);
  }
}
