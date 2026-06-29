# `ogg-opus-decoder`

`ogg-opus-decoder` is a Web Assembly Ogg Opus audio decoder
  * 114.3 KiB minified bundle size (without machine learning enhancements)
  * Uses the latest Opus 1.5 machine learning enhancements for high quality speech decoding
    * Note: Bundle size increases to 4.0 MiB when machine learning enhancements are enabled
  * Browser and NodeJS support
  * Built in Web Worker support
  * Multichannel decoding (up to 255 channels)
  * Based on [`libopus`](https://github.com/xiph/opus) and [`codec-parser`](https://github.com/eshaz/codec-parser)

See the [homepage](https://github.com/eshaz/wasm-audio-decoders) of this repository for more Web Assembly audio decoders like this one.

### [Checkout the demo here](https://eshaz.github.io/wasm-audio-decoders/)

## Installing
* Install from [NPM](https://www.npmjs.com/package/ogg-opus-decoder).

  Run `npm i ogg-opus-decoder`

  ```javascript
  import { OggOpusDecoder } from 'ogg-opus-decoder';

  const decoder = new OggOpusDecoder();
  ```
 
* Or download the build files [here](https://github.com/eshaz/wasm-audio-decoders/tree/main/src/ogg-opus-decoder/dist) and load `ogg-opus-decoder.min.js` in script tag.
  ```html
  <script src="ogg-opus-decoder.min.js" charset="UTF-8"></script>
  <script>
    const decoder = new window["ogg-opus-decoder"].OggOpusDecoder();
  </script>
  ```
  * The `ogg-opus-decoder.opus-ml.min.js` build file is loaded at runtime when `options.speechQualityEnhancement` is set to `"lace"` or `"nolace"`. You may exclude this file if you don't use this option.

* Note that the script must be read using UTF-8 character encoding. If running in a browser, the `<script>` tag must include the `charset="UTF-8"` attribute; or, the loading HTML must include `<meta charset="utf-8" />`.

## Usage

1. Create a new instance and wait for the WASM to finish compiling. Decoding can be done on the main thread synchronously, or in a webworker asynchronously.

   **Main thread synchronous decoding**
   ```javascript
   import { OggOpusDecoder } from 'ogg-opus-decoder';

   const decoder = new OggOpusDecoder();

   // wait for the WASM to be compiled
   await decoder.ready;
   ```

   **Web Worker asynchronous decoding**
   ```javascript
   import { OggOpusDecoderWebWorker } from 'ogg-opus-decoder';

   const decoder = new OggOpusDecoderWebWorker();

   // wait for the WASM to be compiled
   await decoder.ready;
   ```

1. Begin decoding Ogg Opus data.

   ```javascript  
   // Decode an individual Opus frame
   const {channelData, samplesDecoded, sampleRate} = decoder.decode(oggOpusData);
   ```

1. When done decoding, reset the decoder to decode a new stream, or free up the memory being used by the WASM module if you have no more audio to decode. 

   ```javascript
   // `reset()` clears the decoder state and allows you do decode a new stream of Ogg Opus data.
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

## `OggOpusDecoder`

Class that decodes Ogg Opus data synchronously on the main thread.

### Options
```javascript
const decoder = new OggOpusDecoder({ forceStereo: true, speechQualityEnhancement: "nolace" });
```

* `forceStereo` *optional, defaults to `false`*
  * Set to `true` to force stereo output when decoding mono or multichannel Ogg Opus.
  * If there are more than 8 channels, this option is ignored.
* `speechQualityEnhancement` *optional, defaults to `"none"`*
  * `"none"`: (default)
    * Disables machine learning decoding.
    * Normal quality, but fastest.
  * `"lace"`:
    * Enables LACE machine learning decoding.
    * Medium quality, but faster (complexity of 100 MFLOPS)
  * `"nolace"`:
    * Enables Non-Linear LACE machine learning decoding.
    * Best quality, but slower (complexity of 400 MFLOPS).
  * **Note**: Web Assembly SIMD instruction support is required for `"lace"` and `"nolace"`. These options will automatically fallback to `"none"` if SIMD is not supported.
  * See the [opus-1.5 release notes](https://opus-codec.org/demo/opus-1.5/) for more information about machine learning decoding.
* `sampleRate` *optional, defaults to `48000`*
  * Sample rate the decoder will output.
  * Valid sample rates: `8000, 12000, 16000, 24000, or 48000`

### Getters
* `decoder.ready` *async*
  * Returns a promise that is resolved when the WASM is compiled and ready to use.

### Methods

* `decoder.decode(oggOpusData)` *async*
  * `oggOpusData` Uint8Array containing Ogg Opus data.
  * Returns a promise that resolves with the decoded audio.
  * Use this when streaming audio into the decoder.
* `decoder.decodeFile(oggOpusData)` *async*
  * `oggOpusData` Uint8Array containing Ogg Opus data.
  * Returns a promise that resolves with the decoded audio.
  * Use this when decoding an entire file.
* `decoder.flush()` *async*
  * Returns a promise that resolves with any remaining data in the buffer.
  * Use this when you are finished piping audio in through the `decode` method to retrieve any remaining data in the buffer.
* `decoder.reset()` *async*
  * Resets the decoder so that a new stream of Ogg Opus data can be decoded.
* `decoder.free()`
  * De-allocates the memory used by the decoder.
  * After calling `free()`, the current instance is made unusable, and a new instance will need to be created to decode additional Ogg Opus data.

## `OggOpusDecoderWebWorker`

Class that decodes Ogg Opus data asynchronously within a web worker. Decoding is performed in a separate, non-blocking thread. Each new instance spawns a new worker allowing you to run multiple workers for concurrent decoding of multiple streams.

### Options
```javascript
const decoder = new OggOpusDecoderWebWorker({ forceStereo: true, speechQualityEnhancement: "nolace" });
```

* `forceStereo` *optional, defaults to `false`*
  * Set to `true` to force stereo output when decoding mono or multichannel Ogg Opus.
  * If there are more than 8 channels, this option is ignored.
* `speechQualityEnhancement` *optional, defaults to `"none"`*
  * `"none"`: (default)
    * Disables machine learning decoding.
    * Normal quality, but fastest.
  * `"lace"`:
    * Enables LACE machine learning decoding.
    * Medium quality, but faster (complexity of 100 MFLOPS)
  * `"nolace"`:
    * Enables Non-Linear LACE machine learning decoding.
    * Best quality, but slower (complexity of 400 MFLOPS).
  * **Note**: Web Assembly SIMD instruction support is required for `"lace"` and `"nolace"`. These options will automatically fallback to `"none"` if SIMD is not supported.
  * See the [opus-1.5 release notes](https://opus-codec.org/demo/opus-1.5/) for more information about machine learning decoding.
* `sampleRate` *optional, defaults to `48000`*
  * Sample rate the decoder will output.
  * Valid sample rates: `8000, 12000, 16000, 24000, or 48000`

### Getters
* `decoder.ready` *async*
  * Returns a promise that is resolved when the WASM is compiled and ready to use.

### Methods

* `decoder.decode(oggOpusData)` *async*
  * `oggOpusData` Uint8Array containing Ogg Opus data.
  * Returns a promise that resolves with the decoded audio.
  * Use this when streaming audio into the decoder.
* `decoder.decodeFile(oggOpusData)` *async*
  * `oggOpusData` Uint8Array containing Ogg Opus data.
  * Returns a promise that resolves with the decoded audio.
  * Use this when decoding an entire file.
* `decoder.flush()` *async*
  * Returns a promise that resolves with any remaining data in the buffer.
  * Use this when you are finished piping audio in through the `decode` method to retrieve any remaining data in the buffer.
* `decoder.reset()` *async*
  * Resets the decoder so that a new stream of Ogg Opus data can be decoded.
* `decoder.free()` *async*
  * De-allocates the memory used by the decoder and terminates the web worker.
  * After calling `free()`, the current instance is made unusable, and a new instance will need to be created to decode additional Ogg Opus data.

### Properly using the Web Worker interface

`OggOpusDecoderWebWorker` uses async functions to send operations to the web worker without blocking the main thread. To fully take advantage of the concurrency provided by web workers, your code should avoid using `await` on decode operations where it will block the main thread.

Each method call on a `OggOpusDecoderWebWorker` instance will queue up an operation to the web worker. Operations will complete within the web worker thread one at a time and in the same order in which the methods were called.

  * **Good** Main thread is not blocked during each decode operation. The example `playAudio` function is called when each decode operation completes. Also, the next decode operation can begin while `playAudio` is doing work on the main thread.
    ```javascript
    const playAudio = ({ channelData, samplesDecoded, sampleRate }) => {
      // does something to play the audio data.
    }

    decoder.decode(data1).then(playAudio);
    decoder.decode(data2).then(playAudio);
    decoder.decode(data3).then(playAudio);

    // do some other operations while the audio is decoded
    ```

  * **Bad** Main thread is being blocked by `await` during each decode operation. Synchronous code is halted while decoding completes, negating the benefits of using a webworker.
    ```javascript
    const decoded1 = await decoder.decode(data1); // blocks the main thread
    playAudio(decoded1);

    const decoded2 = await decoder.decode(data2); // blocks the main thread
    playAudio(decoded2);

    const decoded3 = await decoder.decode(data3); // blocks the main thread
    playAudio(decoded3);
    ```
## Examples

### Decoding multiple files using a **single** instance of `OggOpusDecoderWebWorker`

This example shows how to decode multiple files using a single `OggOpusDecoderWebWorker` instance. This code iterates over an array of input files (Array of Uint8Arrays) and queues up each file to be decoded one at a time.

First, wait for the decoder to become ready by calling `decoder.ready`.

For each iteration, `decode()` is called, it's result is pushed to the `decodedFiles` array, and `decoder.reset()` is called to prepare the decoder for a new file. These operations are queued up to the decoder instance and will complete one after another.

Finally, a call to `decoder.free()` is queued to clean up the memory stored by the decoder. This resolves when it and all of the other operations before it complete.

It's important to note that there is only one `await` operations in this example. Decoding can happen asynchronously and you only need to `await` when you need to use the results of the decode operation.

```javascript
  const inputFiles = [file1, file2, file3] // Array of Uint8Array file data

  const decoder = new OggOpusDecoderWebWorker();

  const decodedFiles = [];

  const decodePromise = decoder.ready // wait for the decoder to be ready
    .then(() => {
      for (const file of inputFiles) {
        decoder.decode(file) // queue the decode operation
          .then((result) => decodedFiles.push(result)); // save the decode result after decode completes
        decoder.reset(); // queue the reset operation
      }
    })
    .then(() => decoder.free()); // queue the free operation that will execute after the above operations

  // do sync operations here

  // await when you need to have the all of the audio data decoded
  await decodePromise;
```
### Decoding multiple files using **multiple** instances of `OggOpusDecoderWebWorker`

This example shows how to decode multiple files using multiple instances of `OggOpusDecoderWebWorker`. This code iterates over an array of input files (Array of Uint8Arrays) and spawns a new `OggOpusDecoderWebWorker` instance for each file and decodes the file. If you want to take full advantage of multi-core devices, this is the approach you will want to take since it will parallelize the decoding

For each input file, a new decoder is created, and the file is decoded using the `decode()` after  `decoder.ready` is resolved. The result of the `decode()` operation is returned, and a `finally()` function on the promise calls `decoder.free()` to free up the instance after the decode operations are completed.

Finally, `Promise.all()` wraps this array of promises and resolves when all decode operations are complete.

It's important to note that there is only one `await` operation in this example. Decoding can happen asynchronously and you only need to `await` when you need to use the results of the decode operation.

```javascript
  const inputFiles = [file1, file2, file3] // Array of Uint8Array file data

  // loops through each Uint8Array in `inputFiles` and decodes the files in separate threads
  const decodePromise = Promise.all(
    inputFiles.map((file) => {
      const decoder = new OggOpusDecoderWebWorker();

      return decoder.ready
        .then(() => decoder.decode(file)) // decode the input file
        .finally(() => decoder.free()); // free the decoder after resolving the decode result
    })
  );

  // do sync operations here

  // await when you need to have the all of the audio data decoded
  const decodedFiles = await decodePromise;
```