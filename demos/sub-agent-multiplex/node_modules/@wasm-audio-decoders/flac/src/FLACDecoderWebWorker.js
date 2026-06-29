import { WASMAudioDecoderWorker } from "@wasm-audio-decoders/common";
import EmscriptenWASM from "./EmscriptenWasm.js";
import FLACDecoder, { Decoder, setDecoderClass } from "./FLACDecoder.js";

class DecoderWorker extends WASMAudioDecoderWorker {
  constructor(options) {
    super(options, "flac-decoder", Decoder, EmscriptenWASM);
  }

  async decodeFrames(frames) {
    return this.postToDecoder("decodeFrames", frames);
  }
}

export default class FLACDecoderWebWorker extends FLACDecoder {
  constructor() {
    super();

    super[setDecoderClass](DecoderWorker);
  }

  async free() {
    await this._decoder.free();
  }

  terminate() {
    this._decoder.terminate();
  }
}
