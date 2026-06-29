import { WASMAudioDecoderCommon } from "@wasm-audio-decoders/common";

import EmscriptenWASM from "./EmscriptenWasm.js";

export default function MPEGDecoder(options = {}) {
  // injects dependencies when running as a web worker
  // async
  this._init = () => {
    return new this._WASMAudioDecoderCommon()
      .instantiate(this._EmscriptenWASM, this._module)
      .then((common) => {
        this._common = common;

        this._sampleRate = 0;
        this._inputBytes = 0;
        this._outputSamples = 0;
        this._frameNumber = 0;

        this._input = this._common.allocateTypedArray(
          this._inputSize,
          Uint8Array,
        );

        this._output = this._common.allocateTypedArray(
          this._outputSize,
          Float32Array,
        );

        const decoderPtr = this._common.allocateTypedArray(1, Uint32Array);
        this._samplesDecodedPtr = this._common.allocateTypedArray(
          1,
          Uint32Array,
        );
        this._sampleRatePtr = this._common.allocateTypedArray(1, Uint32Array);
        this._errorStringPtr = this._common.allocateTypedArray(1, Uint32Array);

        const error = this._common.wasm.mpeg_frame_decoder_create(
          decoderPtr.ptr,
          options.enableGapless === false ? 0 : 1, // default to enabled
        );

        if (error) {
          throw Error(this._getErrorMessage(error));
        }

        this._decoder = decoderPtr.buf[0];
      });
  };

  Object.defineProperty(this, "ready", {
    enumerable: true,
    get: () => this._ready,
  });

  this._getErrorMessage = (error) =>
    error + " " + this._common.codeToString(this._errorStringPtr.buf[0]);

  // async
  this.reset = () => {
    this.free();
    return this._init();
  };

  this.free = () => {
    this._common.wasm.mpeg_frame_decoder_destroy(this._decoder);
    this._common.wasm.free(this._decoder);

    this._common.free();
  };

  this.decode = (data) => {
    let output = [],
      errors = [],
      samples = 0;

    if (!(data instanceof Uint8Array))
      throw Error(
        "Data to decode must be Uint8Array. Instead got " + typeof data,
      );

    feed: for (
      let dataOffset = 0, dataChunkLength = 0;
      dataOffset < data.length;
      dataOffset += dataChunkLength
    ) {
      const dataChunk = data.subarray(dataOffset, this._input.len + dataOffset);
      dataChunkLength = dataChunk.length;
      this._inputBytes += dataChunkLength;

      this._input.buf.set(dataChunk);

      // feed data in chunks as large as the input buffer
      let error = this._common.wasm.mpeg_decoder_feed(
        this._decoder,
        this._input.ptr,
        dataChunkLength,
      );

      if (error === -10) {
        continue feed; // MPG123_NEED_MORE
      }

      // decode data in chunks as large as the input buffer
      read: while (true) {
        this._samplesDecodedPtr.buf[0] = 0;

        error = this._common.wasm.mpeg_decoder_read(
          this._decoder,
          this._output.ptr,
          this._output.len,
          this._samplesDecodedPtr.ptr,
          this._sampleRatePtr.ptr,
          this._errorStringPtr.ptr,
        );

        const samplesDecoded = this._samplesDecodedPtr.buf[0];
        this._outputSamples += samplesDecoded;

        if (samplesDecoded) {
          samples += samplesDecoded;
          output.push([
            this._output.buf.slice(0, samplesDecoded),
            this._output.buf.slice(samplesDecoded, samplesDecoded * 2),
          ]);
        }

        if (error == -11) {
          continue read; // MPG123_NEW_FORMAT, usually the start of a new stream
        } else if (error === -10) {
          continue feed; // MPG123_NEED_MORE
        } else if (error) {
          const message = this._getErrorMessage(error);
          console.error("mpg123-decoder: " + message);

          this._common.addError(
            errors,
            message,
            0,
            this._frameNumber,
            this._inputBytes,
            this._outputSamples,
          );
        }
      }
    }

    return this._WASMAudioDecoderCommon.getDecodedAudioMultiChannel(
      errors,
      output,
      2,
      samples,
      this._sampleRatePtr.buf[0],
    );
  };

  this.decodeFrame = (mpegFrame) => {
    const decoded = this.decode(mpegFrame);
    this._frameNumber++;
    return decoded;
  };

  this.decodeFrames = (mpegFrames) => {
    let output = [],
      errors = [],
      samples = 0,
      i = 0;

    while (i < mpegFrames.length) {
      const decoded = this.decodeFrame(mpegFrames[i++]);

      output.push(decoded.channelData);
      errors = errors.concat(decoded.errors);
      samples += decoded.samplesDecoded;
    }

    return this._WASMAudioDecoderCommon.getDecodedAudioMultiChannel(
      errors,
      output,
      2,
      samples,
      this._sampleRatePtr.buf[0],
    );
  };

  // constructor

  // injects dependencies when running as a web worker
  this._isWebWorker = MPEGDecoder.isWebWorker;
  this._WASMAudioDecoderCommon =
    MPEGDecoder.WASMAudioDecoderCommon || WASMAudioDecoderCommon;
  this._EmscriptenWASM = MPEGDecoder.EmscriptenWASM || EmscriptenWASM;
  this._module = MPEGDecoder.module;

  this._inputSize = 2 ** 16;
  this._outputSize = 2889 * 16 * 2;

  this._ready = this._init();

  return this;
}
