import { WASMAudioDecoderCommon } from "@wasm-audio-decoders/common";
import CodecParser, {
  data,
  totalSamples,
  codecFrames,
  isLastPage,
} from "codec-parser";

import EmscriptenWASM from "./EmscriptenWasm.js";

export function Decoder() {
  // injects dependencies when running as a web worker
  // async
  this._init = () => {
    return new this._WASMAudioDecoderCommon()
      .instantiate(this._EmscriptenWASM, this._module)
      .then((common) => {
        this._common = common;

        this._inputBytes = 0;
        this._outputSamples = 0;
        this._frameNumber = 0;

        this._channels = this._common.allocateTypedArray(1, Uint32Array);
        this._sampleRate = this._common.allocateTypedArray(1, Uint32Array);
        this._bitsPerSample = this._common.allocateTypedArray(1, Uint32Array);
        this._samplesDecoded = this._common.allocateTypedArray(1, Uint32Array);
        this._outputBufferPtr = this._common.allocateTypedArray(1, Uint32Array);
        this._outputBufferLen = this._common.allocateTypedArray(1, Uint32Array);

        this._errorStringPtr = this._common.allocateTypedArray(1, Uint32Array);
        this._stateStringPtr = this._common.allocateTypedArray(1, Uint32Array);

        this._decoder = this._common.wasm.create_decoder(
          this._channels.ptr,
          this._sampleRate.ptr,
          this._bitsPerSample.ptr,
          this._samplesDecoded.ptr,
          this._outputBufferPtr.ptr,
          this._outputBufferLen.ptr,
          this._errorStringPtr.ptr,
          this._stateStringPtr.ptr,
        );
      });
  };

  Object.defineProperty(this, "ready", {
    enumerable: true,
    get: () => this._ready,
  });

  // async
  this.reset = () => {
    this.free();
    return this._init();
  };

  this.free = () => {
    this._common.wasm.destroy_decoder(this._decoder);

    this._common.free();
  };

  this._decode = (data) => {
    if (!(data instanceof Uint8Array))
      throw Error(
        "Data to decode must be Uint8Array. Instead got " + typeof data,
      );

    const input = this._common.allocateTypedArray(
      data.length,
      Uint8Array,
      false,
    );
    input.buf.set(data);

    this._common.wasm.decode_frame(this._decoder, input.ptr, input.len);

    let errorMessage = [],
      error;
    if (this._errorStringPtr.buf[0])
      errorMessage.push(
        "Error: " + this._common.codeToString(this._errorStringPtr.buf[0]),
      );

    if (this._stateStringPtr.buf[0])
      errorMessage.push(
        "State: " + this._common.codeToString(this._stateStringPtr.buf[0]),
      );

    if (errorMessage.length) {
      error = errorMessage.join("; ");
      console.error(
        "@wasm-audio-decoders/flac: \n\t" + errorMessage.join("\n\t"),
      );
    }

    const output = new Float32Array(
      this._common.wasm.HEAP,
      this._outputBufferPtr.buf[0],
      this._outputBufferLen.buf[0],
    );

    const decoded = {
      error: error,
      outputBuffer: this._common.getOutputChannels(
        output,
        this._channels.buf[0],
        this._samplesDecoded.buf[0],
      ),
      samplesDecoded: this._samplesDecoded.buf[0],
    };

    this._common.wasm.free(this._outputBufferPtr.buf[0]);
    this._outputBufferLen.buf[0] = 0;
    this._samplesDecoded.buf[0] = 0;

    return decoded;
  };

  this.decodeFrames = (frames) => {
    let outputBuffers = [],
      errors = [],
      outputSamples = 0;

    for (let i = 0; i < frames.length; i++) {
      let offset = 0;
      const data = frames[i];

      while (offset < data.length) {
        const chunk = data.subarray(offset, offset + this._MAX_INPUT_SIZE);
        offset += chunk.length;

        const decoded = this._decode(chunk);

        outputBuffers.push(decoded.outputBuffer);
        outputSamples += decoded.samplesDecoded;

        if (decoded.error)
          this._common.addError(
            errors,
            decoded.error,
            data.length,
            this._frameNumber,
            this._inputBytes,
            this._outputSamples,
          );

        this._inputBytes += data.length;
        this._outputSamples += decoded.samplesDecoded;
      }

      this._frameNumber++;
    }

    return this._WASMAudioDecoderCommon.getDecodedAudioMultiChannel(
      errors,
      outputBuffers,
      this._channels.buf[0],
      outputSamples,
      this._sampleRate.buf[0],
      this._bitsPerSample.buf[0],
    );
  };

  // injects dependencies when running as a web worker
  this._isWebWorker = Decoder.isWebWorker;
  this._WASMAudioDecoderCommon =
    Decoder.WASMAudioDecoderCommon || WASMAudioDecoderCommon;
  this._EmscriptenWASM = Decoder.EmscriptenWASM || EmscriptenWASM;
  this._module = Decoder.module;

  this._MAX_INPUT_SIZE = 65535 * 8;

  this._ready = this._init();

  return this;
}

export const setDecoderClass = Symbol();

const determineDecodeMethod = Symbol();
const decodeFlac = Symbol();
const decodeOggFlac = Symbol();
const placeholderDecodeMethod = Symbol();
const decodeMethod = Symbol();
const init = Symbol();
const totalSamplesDecoded = Symbol();

export default class FLACDecoder {
  constructor() {
    this._onCodec = (codec) => {
      if (codec !== "flac")
        throw new Error(
          "@wasm-audio-decoders/flac does not support this codec " + codec,
        );
    };

    // instantiate to create static properties
    new WASMAudioDecoderCommon();

    this[init]();
    this[setDecoderClass](Decoder);
  }

  [init]() {
    this[decodeMethod] = placeholderDecodeMethod;
    this[totalSamplesDecoded] = 0;
    this._codecParser = null;
  }

  [determineDecodeMethod](data) {
    if (!this._codecParser && data.length >= 4) {
      let codec = "audio/";

      if (
        data[0] !== 0x4f || // O
        data[1] !== 0x67 || // g
        data[2] !== 0x67 || // g
        data[3] !== 0x53 //    S
      ) {
        codec += "flac";
        this[decodeMethod] = decodeFlac;
      } else {
        codec += "ogg";
        this[decodeMethod] = decodeOggFlac;
      }

      this._codecParser = new CodecParser(codec, {
        onCodec: this._onCodec,
        enableFrameCRC32: false,
      });
    }
  }

  [setDecoderClass](decoderClass) {
    if (this._decoder) {
      const oldDecoder = this._decoder;
      oldDecoder.ready.then(() => oldDecoder.free());
    }

    this._decoder = new decoderClass();
    this._ready = this._decoder.ready;
  }

  [decodeFlac](flacFrames) {
    return this._decoder.decodeFrames(flacFrames.map((f) => f[data] || f));
  }

  [decodeOggFlac](oggPages) {
    const frames = oggPages
      .map((page) => page[codecFrames].map((f) => f[data]))
      .flat();

    const decoded = this._decoder.decodeFrames(frames);

    const oggPage = oggPages[oggPages.length - 1];
    if (oggPage && oggPage[isLastPage]) {
      // trim any extra samples that are decoded beyond the absoluteGranulePosition, relative to where we started in the stream
      const samplesToTrim = this[totalSamplesDecoded] - oggPage[totalSamples];

      if (samplesToTrim > 0) {
        for (let i = 0; i < decoded.channelData.length; i++)
          decoded.channelData[i] = decoded.channelData[i].subarray(
            0,
            decoded.samplesDecoded - samplesToTrim,
          );

        decoded.samplesDecoded -= samplesToTrim;
      }
    }

    this[totalSamplesDecoded] += decoded.samplesDecoded;

    return decoded;
  }

  [placeholderDecodeMethod]() {
    return WASMAudioDecoderCommon.getDecodedAudio([], [], 0, 0, 0);
  }

  get ready() {
    return this._ready;
  }

  async reset() {
    this[init]();
    return this._decoder.reset();
  }

  free() {
    this._decoder.free();
  }

  async decode(flacData) {
    if (this[decodeMethod] === placeholderDecodeMethod)
      this[determineDecodeMethod](flacData);

    return this[this[decodeMethod]]([
      ...this._codecParser.parseChunk(flacData),
    ]);
  }

  async flush() {
    const decoded = this[this[decodeMethod]]([...this._codecParser.flush()]);

    await this.reset();
    return decoded;
  }

  async decodeFile(flacData) {
    this[determineDecodeMethod](flacData);

    const decoded = this[this[decodeMethod]]([
      ...this._codecParser.parseAll(flacData),
    ]);

    await this.reset();
    return decoded;
  }

  async decodeFrames(flacFrames) {
    return this[decodeFlac](flacFrames);
  }
}
