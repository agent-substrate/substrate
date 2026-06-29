import { WASMAudioDecoderWorker } from "@wasm-audio-decoders/common";
import EmscriptenWASM from "./EmscriptenWasm.js";
import OpusMLDecoder from "./OpusMLDecoder.js";

export default class OpusMLDecoderWebWorker extends WASMAudioDecoderWorker {
  constructor(options) {
    super(options, "opus-ml", OpusMLDecoder, EmscriptenWASM);
  }

  async decodeFrame(data) {
    return this.postToDecoder("decodeFrame", data);
  }

  async decodeFrames(data) {
    return this.postToDecoder("decodeFrames", data);
  }
}
