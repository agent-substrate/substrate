# `@wasm-audio-decoders/opus-ml`

`@wasm-audio-decoders/opus-ml` is a Web Assembly Opus audio decoder compiled with machine learning enhancements.
  * 3.9 MiB minified bundle size
  * Uses the latest [Opus 1.5 machine learning enhancements](https://opus-codec.org/demo/opus-1.5/) for high quality speech decoding
  * Browser and NodeJS support (WASM SIMD support required)
  * Built in Web Worker support
  * Multichannel decoding (up to 255 channels)
  * Based on [`libopus`](https://github.com/xiph/opus)
  * **Notes**
    * Web Assembly SIMD instruction support is required for this library. Please use [`opus-decoder`](https://github.com/eshaz/wasm-audio-decoders/tree/main/src/opus-decoder) or [`ogg-opus-decoder`](https://github.com/eshaz/wasm-audio-decoders/tree/main/src/ogg-opus-decoder) if your platform does not support SIMD.
    * For a smaller library, see [`opus-decoder`](https://github.com/eshaz/wasm-audio-decoders/tree/main/src/opus-decoder) which is compiled without the machine learning enhancements.

This library is intended for users that already have Opus frames extracted from a container, i.e. (Ogg, Matroska (WEBM), or ISOBMFF (mp4)). See [`ogg-opus-decoder`](https://github.com/eshaz/wasm-audio-decoders/tree/main/src/ogg-opus-decoder) if you have an Ogg Opus file to decode.

See the [homepage](https://github.com/eshaz/wasm-audio-decoders) of this repository for more Web Assembly audio decoders like this one.

### [Checkout the demo here](https://eshaz.github.io/wasm-audio-decoders/)

## Installing
* Install from [NPM](https://www.npmjs.com/package/opus-ml).

  Run `npm i opus-ml`

  ```javascript
  import { OpusMLDecoder } from '@wasm-audio-decoders/opus-ml';

  const decoder = new OpusMLDecoder();
  ```
 
* Or download the [build](https://github.com/eshaz/wasm-audio-decoders/tree/main/src/opus-ml/dist) and include it as a script.
  ```html
  <script src="opus-ml-decoder.min.js" charset="UTF-8"></script>
  <script>
    const decoder = new window["opus-ml"].OpusMLDecoder();
  </script>
  ```
* Note that the script must be read using UTF-8 character encoding. If running in a browser, the `<script>` tag must include the `charset="UTF-8"` attribute; or, the loading HTML must include `<meta charset="utf-8" />`.

## Usage

1. Create a new instance and wait for the WASM to finish compiling. Decoding can be done on the main thread synchronously, or in a webworker asynchronously.

   **Main thread synchronous decoding**
   ```javascript
   import { OpusMLDecoder } from '@wasm-audio-decoders/opus-ml';

   const decoder = new OpusMLDecoder();

   // wait for the WASM to be compiled
   await decoder.ready;
   ```

   **Web Worker asynchronous decoding**
   ```javascript
   import { OpusMLDecoderWebWorker } from '@wasm-audio-decoders/opus-ml';

   const decoder = new OpusMLDecoderWebWorker();

   // wait for the WASM to be compiled
   await decoder.ready;
   ```

1. Begin decoding Opus frames.

   ```javascript  
   // Decode an individual Opus frame
   const {channelData, samplesDecoded, sampleRate} = decoder.decodeFrame(opusFrame);
   
   // Decode an array of individual Opus frames
   const {channelData, samplesDecoded, sampleRate} = decoder.decodeFrames(opusFrameArray);
   ```

1. When done decoding, reset the decoder to decode a new stream, or free up the memory being used by the WASM module if you have no more audio to decode. 

   ```javascript
   // `reset()` clears the decoder state and allows you do decode a new stream of Opus frames.
   await decoder.reset();

   // `free()` de-allocates the memory used by the decoder. You will need to create a new instance after calling `free()` to start decoding again.
   decoder.free();
   ```

## API
Decoded audio is always returned in the below structure.

```javascript
{
    channelData: [
      leftAudio, // Float32Array of PCM samples for the left channel
      rightAudio, // Float32Array of PCM samples for the right channel
      ... // additional channels
    ],
    samplesDecoded: 1234, // number of PCM samples that were decoded per channel
    sampleRate: 48000, // sample rate of the decoded PCM
    errors: [ // array containing descriptions for any decode errors
      {
        message: "libopus -4 OPUS_INVALID_PACKET: The compressed data passed is corrupted",
        frameLength: 400, // length of the frame or data in bytes that encountered an error
        frameNumber: 21, // position of error relative to total frames decoded 
        inputBytes: 4905, // position of error relative to total input bytes
        outputSamples: 18888, // position of error relative to total output samples
      }
    ]
}
```

Each Float32Array within `channelData` can be used directly in the WebAudio API for playback.

Decoding will proceed through any errors. Any errors encountered may result in gaps in the decoded audio.

### Multichannel Output

Each channel is assigned to a speaker location in a conventional surround arrangement. Specific locations depend on the number of channels, and are given below in order of the corresponding channel indices. This set of surround options and speaker location orderings is the same as those used by the Vorbis codec.

* 1 channel: monophonic (mono).
* 2 channels: stereo (left, right).
* 3 channels: linear surround (left, center, right).
* 4 channels: quadraphonic (front left, front right, rear left, rear right).
* 5 channels: 5.0 surround (front left, front center, front right, rear left, rear right).
* 6 channels: 5.1 surround (front left, front center, front right, rear left, rear right, LFE).
* 7 channels: 6.1 surround (front left, front center, front right, side left, side right, rear center, LFE).
* 8 channels: 7.1 surround (front left, front center, front right, side left, side right, rear left, rear right, LFE).
* 9-255 channels: No mapping is defined.

See: https://datatracker.ietf.org/doc/html/rfc7845.html#section-5.1.1.2

Each Float32Array within `channelData` can be used directly in the WebAudio API for playback.

## `OpusMLDecoder`

Class that decodes Opus frames synchronously on the main thread.

### Options
```javascript
const decoder = new OpusMLDecoder({
  speechQualityEnhancement: "nolace",
  forceStereo: false,
  sampleRate: 48000,
  preSkip: 0,
  channels: 2,
  streamCount: 1,
  coupledStreamCount: 1,
  channelMappingTable: [0, 1]
});
```

#### **The below options should be obtained from the Opus Header.**
See this [documentation](https://wiki.xiph.org/OggOpus#ID_Header) on the Opus header for more information. If you don't have access to the Opus header, the default values will successfully decode most stereo Opus streams.

* `speechQualityEnhancement` *optional, defaults to `"nolace"`*
  * `"none"`:
    * Disables machine learning decoding.
    * Normal quality, but fastest.
  * `"lace"`:
    * Enables LACE machine learning decoding.
    * Medium quality, but faster (complexity of 100 MFLOPS)
  * `"nolace"`: (default)
    * Enables Non-Linear LACE machine learning decoding.
    * Best quality, but slower (complexity of 400 MFLOPS).
  * See the [opus-1.5 release notes](https://opus-codec.org/demo/opus-1.5/) for more information about machine learning decoding.
* `forceStereo` *optional, defaults to `false`*
  * Set to `true` to force stereo output when decoding mono or multichannel Ogg Opus.
  * If there are more than 8 channels, this option is ignored.
* `preSkip` *optional, defaults to `0`*
  * Number of samples to skip at the beginning reported by the Opus header.
* `sampleRate` *optional, defaults to `48000`*
  * Sample rate the decoder will output.
  * Valid sample rates: `8000, 12000, 16000, 24000, or 48000`
#### ***Required for Multichannel Decoding.** (Channel Mapping Family >= 1)*
* `channels` *optional, defaults to `2`*
  * Number of channels reported by the Opus header.
* `streamCount` *optional, defaults to `1`*
  * Number of streams reported by the Opus header.
* `coupledStreamCount` *optional, defaults to: `1` when 2 channels, `0` when 1 channel*
  * Number of coupled streams reported by the Opus header.
* `channelMappingTable` *optional, defaults to `[0, 1]` when 2 channels, `[0]` when 1 channel*
  * Channel mapping reported by the Opus header.

### Getters
* `decoder.ready` *async*
  * Returns a promise that is resolved when the WASM is compiled and ready to use.

### Methods

* `decoder.decodeFrame(opusFrame)`
  * `opusFrame` Uint8Array containing a single Opus frame.
  * Returns decoded audio.
* `decoder.decodeFrames(opusFrames)`
  * `opusFrames` Array of Uint8Arrays containing Opus frames.
  * Returns decoded audio.
* `decoder.reset()` *async*
  * Resets the decoder so that a new stream of Opus frames can be decoded.
* `decoder.free()`
  * De-allocates the memory used by the decoder.
  * After calling `free()`, the current instance is made unusable, and a new instance will need to be created to decode additional Opus frames.

## `OpusMLDecoderWebWorker`

Class that decodes Opus frames asynchronously within a web worker. Decoding is performed in a separate, non-blocking thread. Each new instance spawns a new worker allowing you to run multiple workers for concurrent decoding of multiple streams.

### Options
```javascript
const decoder = new OpusMLDecoderWebWorker({ 
  speechQualityEnhancement: "nolace",
  forceStereo: false,
  sampleRate: 48000,
  channels: 2,
  streamCount: 1,
  coupledStreamCount: 1,
  channelMappingTable: [0, 1]
});
```

#### **The below options should be obtained from the Opus Header.**
See this [documentation](https://wiki.xiph.org/OggOpus#ID_Header) on the Opus header for more information. If you don't have access to the Opus header, the default values will successfully decode most stereo Opus streams.

* `speechQualityEnhancement` *optional, defaults to `"nolace"`*
  * `"none"`:
    * Disables machine learning decoding.
    * Normal quality, but fastest.
  * `"lace"`:
    * Enables LACE machine learning decoding.
    * Medium quality, but faster (complexity of 100 MFLOPS)
  * `"nolace"`: (default)
    * Enables Non-Linear LACE machine learning decoding.
    * Best quality, but slower (complexity of 400 MFLOPS).
  * See the [opus-1.5 release notes](https://opus-codec.org/demo/opus-1.5/) for more information about machine learning decoding.
* `forceStereo` *optional, defaults to `false`*
  * Set to `true` to force stereo output when decoding mono or multichannel Ogg Opus.
  * If there are more than 8 channels, this option is ignored.
* `preSkip` *optional, defaults to `0`*
  * Number of samples to skip at the beginning reported by the Opus header.
* `sampleRate` *optional, defaults to `48000`*
  * Sample rate the decoder will output.
  * Valid sample rates: `8000, 12000, 16000, 24000, or 48000`
#### ***Required for Multichannel Decoding.** (Channel Mapping Family >= 1)*
* `channels` *optional, defaults to `2`*
  * Number of channels reported by the Opus header.
* `streamCount` *optional, defaults to `1`*
  * Number of streams reported by the Opus header.
* `coupledStreamCount` *optional, defaults to: `1` when 2 channels, `0` when 1 channel*
  * Number of coupled streams reported by the Opus header.
* `channelMappingTable` *optional, defaults to `[0, 1]` when 2 channels, `[0]` when 1 channel*
  * Channel mapping reported by the Opus header.
### Getters
* `decoder.ready` *async*
  * Returns a promise that is resolved when the WASM is compiled and ready to use.

### Methods

* `decoder.decodeFrame(opusFrame)` *async*
  * `opusFrame` Uint8Array containing a single Opus frame.
  * Returns a promise that resolves with the decoded audio.
* `decoder.decodeFrames(opusFrames)` *async*
  * `opusFrames` Array of Uint8Arrays containing Opus frames.
  * Returns a promise that resolves with the decoded audio.
* `decoder.reset()` *async*
  * Resets the decoder so that a new stream of Opus frames can be decoded.
* `decoder.free()` *async*
  * De-allocates the memory used by the decoder and terminates the web worker.
  * After calling `free()`, the current instance is made unusable, and a new instance will need to be created to decode additional Opus frames.

### Properly using the Web Worker interface

`OpusMLDecoderWebWorker` uses async functions to send operations to the web worker without blocking the main thread. To fully take advantage of the concurrency provided by web workers, your code should avoid using `await` on decode operations where it will block the main thread.

Each method call on a `OpusMLDecoderWebWorker` instance will queue up an operation to the web worker. Operations will complete within the web worker thread one at a time and in the same order in which the methods were called.

  * **Good** Main thread is not blocked during each decode operation. The example `playAudio` function is called when each decode operation completes. Also, the next decode operation can begin while `playAudio` is doing work on the main thread.
    ```javascript
    const playAudio = ({ channelData, samplesDecoded, sampleRate }) => {
      // does something to play the audio data.
    }

    decoder.decodeFrame(frameData1).then(playAudio);
    decoder.decodeFrame(frameData2).then(playAudio);
    decoder.decodeFrame(frameData3).then(playAudio);

    // do some other operations while the audio is decoded
    ```

  * **Bad** Main thread is being blocked by `await` during each decode operation. Synchronous code is halted while decoding completes, negating the benefits of using a webworker.
    ```javascript
    const decoded1 = await decoder.decodeFrame(frameData1); // blocks the main thread
    playAudio(decoded1);

    const decoded2 = await decoder.decodeFrame(frameData2); // blocks the main thread
    playAudio(decoded2);

    const decoded3 = await decoder.decodeFrame(frameData3); // blocks the main thread
    playAudio(decoded3);
    ```