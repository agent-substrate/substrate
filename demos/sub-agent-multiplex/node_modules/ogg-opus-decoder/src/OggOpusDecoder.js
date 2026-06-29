import { WASMAudioDecoderCommon } from "@wasm-audio-decoders/common";
import { OpusDecoder, OpusDecoderWebWorker } from "opus-decoder";
import CodecParser, {
  codecFrames,
  header,
  channels,
  streamCount,
  coupledStreamCount,
  channelMappingTable,
  preSkip,
  isLastPage,
  absoluteGranulePosition,
  data,
  totalSamples,
} from "codec-parser";

// prettier-ignore
const simd=async()=>WebAssembly.validate(new Uint8Array([0,97,115,109,1,0,0,0,1,5,1,96,0,1,123,3,2,1,0,10,10,1,8,0,65,0,253,15,253,98,11]))

export default class OggOpusDecoder {
  constructor(options = {}) {
    this._sampleRate = options.sampleRate || 48000;
    this._speechQualityEnhancement = options.speechQualityEnhancement;
    this._forceStereo =
      options.forceStereo !== undefined ? options.forceStereo : false;

    this._onCodec = (codec) => {
      if (codec !== "opus")
        throw new Error(
          "ogg-opus-decoder does not support this codec " + codec,
        );
    };

    // instantiate to create static properties
    new WASMAudioDecoderCommon();
    this._useMLDecoder = ["lace", "nolace"].includes(
      this._speechQualityEnhancement,
    );

    this._decoderLibraryLoaded = this._loadDecoderLibrary();
    this._ready = this._init();
  }

  _initDecoderClass() {
    this._decoderClass = this._useMLDecoder
      ? this.OpusMLDecoder
      : this.OpusDecoder;
  }

  async _loadDecoderLibrary() {
    if (this._useMLDecoder) {
      const simdSupported = await simd();

      if (simdSupported) {
        const { OpusMLDecoder, OpusMLDecoderWebWorker } = await import(
          /* webpackChunkName: "opus-ml" */ "@wasm-audio-decoders/opus-ml"
        );
        this.OpusMLDecoder = OpusMLDecoder;
        this.OpusMLDecoderWebWorker = OpusMLDecoderWebWorker;
      } else {
        console.warn(
          `ogg-opus-decoder: This platform does not support WebAssembly SIMD; { speechQualityEnhancements: '${this._speechQualityEnhancement}' } has been disabled`,
        );
        this._useMLDecoder = false;
      }
    }
    this.OpusDecoder = OpusDecoder;
    this.OpusDecoderWebWorker = OpusDecoderWebWorker;

    this._initDecoderClass();
  }

  async _init() {
    if (this._decoder) await this._decoder.free();
    this._decoder = null;
    this._decoderReady = null;

    this._codecParser = new CodecParser("application/ogg", {
      onCodec: this._onCodec,
      enableFrameCRC32: false,
    });
  }

  async _instantiateDecoder(header) {
    this._totalSamplesDecoded = 0;
    this._preSkip = header[preSkip];
    this._channels = this._forceStereo ? 2 : header[channels];

    await this._decoderLibraryLoaded;

    this._decoder = new this._decoderClass({
      channels: header[channels],
      streamCount: header[streamCount],
      coupledStreamCount: header[coupledStreamCount],
      channelMappingTable: header[channelMappingTable],
      preSkip: Math.round((this._preSkip / 48000) * this._sampleRate),
      sampleRate: this._sampleRate,
      speechQualityEnhancement: this._speechQualityEnhancement,
      forceStereo: this._forceStereo,
    });
    return this._decoder.ready;
  }

  get ready() {
    return this._ready;
  }

  async reset() {
    this._ready = this._init();
    await this._ready;
  }

  free() {
    if (this._decoder) this._decoder.free();
  }

  async _decode(oggPages) {
    let opusFrames = [],
      allErrors = [],
      allChannelData = [],
      samplesThisDecode = 0;

    const flushFrames = async () => {
      if (opusFrames.length) {
        await this._decoderReady;

        const { channelData, samplesDecoded, errors } =
          await this._decoder.decodeFrames(opusFrames);

        allChannelData.push(channelData);
        allErrors.push(...errors);
        samplesThisDecode += samplesDecoded;
        this._totalSamplesDecoded += samplesDecoded;

        opusFrames = [];
      }
    };

    for (let i = 0; i < oggPages.length; i++) {
      const oggPage = oggPages[i];

      // only decode Ogg pages that have codec frames
      const frames = oggPage[codecFrames].map((f) => f[data]);
      if (frames.length) {
        opusFrames.push(...frames);

        if (!this._decoderReady)
          // wait until there is an Opus header before instantiating
          this._decoderReady = this._instantiateDecoder(
            oggPage[codecFrames][0][header],
          );
      }

      if (oggPage[isLastPage]) {
        // decode anything left in the current ogg file
        await flushFrames();

        // in cases where BigInt isn't supported, don't do any absoluteGranulePosition logic (i.e. old iOS versions)
        if (
          oggPage[absoluteGranulePosition] !== undefined &&
          allChannelData.length
        ) {
          const totalDecodedSamples_48000 =
            (this._totalSamplesDecoded / this._sampleRate) * 48000;

          // trim any extra samples that are decoded beyond the absoluteGranulePosition, relative to where we started in the stream
          const samplesToTrim = Math.round(
            ((totalDecodedSamples_48000 - oggPage[totalSamples]) / 48000) *
              this._sampleRate,
          );

          const channelData = allChannelData[allChannelData.length - 1];
          if (samplesToTrim > 0) {
            for (let i = 0; i < channelData.length; i++) {
              channelData[i] = channelData[i].subarray(
                0,
                channelData[i].length - samplesToTrim,
              );
            }
          }

          samplesThisDecode -= samplesToTrim;
          this._totalSamplesDecoded -= samplesToTrim;
        }

        // reached the end of an ogg stream, reset the decoder
        await this.reset();
      }
    }

    await flushFrames();

    return [
      allErrors,
      allChannelData,
      this._channels,
      samplesThisDecode,
      this._sampleRate,
      16,
    ];
  }

  async decode(oggOpusData) {
    const decoded = await this._decode([
      ...this._codecParser.parseChunk(oggOpusData),
    ]);

    return WASMAudioDecoderCommon.getDecodedAudioMultiChannel(...decoded);
  }

  async decodeFile(oggOpusData) {
    const decoded = await this._decode([
      ...this._codecParser.parseAll(oggOpusData),
    ]);
    await this.reset();

    return WASMAudioDecoderCommon.getDecodedAudioMultiChannel(...decoded);
  }

  async flush() {
    const decoded = await this._decode([...this._codecParser.flush()]);
    await this.reset();

    return WASMAudioDecoderCommon.getDecodedAudioMultiChannel(...decoded);
  }
}
