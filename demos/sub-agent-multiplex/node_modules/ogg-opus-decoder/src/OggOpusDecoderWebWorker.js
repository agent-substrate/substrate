import OggOpusDecoder from "./OggOpusDecoder.js";

export default class OggOpusDecoderWebWorker extends OggOpusDecoder {
  constructor(options) {
    super(options);
  }

  _initDecoderClass() {
    this._decoderClass = this._useMLDecoder
      ? this.OpusMLDecoderWebWorker
      : this.OpusDecoderWebWorker;
  }

  async free() {
    if (this._decoder) await this._decoder.free();
  }
}
