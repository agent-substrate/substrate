import { WASMAudioDecoderWorker } from "@wasm-audio-decoders/common";
import EmscriptenWASM from "./EmscriptenWasm.js";
import OggVorbisDecoder, {
  Decoder,
  setDecoderClass,
} from "./OggVorbisDecoder.js";

class DecoderWorker extends WASMAudioDecoderWorker {
  constructor(options) {
    super(options, "ogg-vorbis-decoder", Decoder, EmscriptenWASM);
  }

  async sendSetupHeader(data) {
    return this.postToDecoder("sendSetupHeader", data);
  }

  async initDsp() {
    return this.postToDecoder("initDsp");
  }

  async decodePackets(packets) {
    return this.postToDecoder("decodePackets", packets);
  }
}

export default class OggVorbisDecoderWebWorker extends OggVorbisDecoder {
  constructor() {
    super();

    this._ready = super[setDecoderClass](DecoderWorker);
  }

  async free() {
    await this._decoder.free();
  }

  terminate() {
    this._decoder.terminate();
  }
}
