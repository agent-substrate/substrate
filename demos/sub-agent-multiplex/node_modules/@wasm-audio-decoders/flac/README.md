# `@wasm-audio-decoders/flac`

`@wasm-audio-decoders/flac` is a Web Assembly FLAC / Ogg FLAC audio decoder.
  * 67.2 KiB minified bundle size
  * Browser and NodeJS support
  * Built in Web Worker support
  * Multichannel decoding (up to 8 channels)
  * Supports full FLAC bit depth and sample rate.
  * Based on [`libFLAC`](https://github.com/xiph/flac) and [`codec-parser`](https://github.com/eshaz/codec-parser)

See the [homepage](https://github.com/eshaz/wasm-audio-decoders) of this repository for more Web Assembly audio decoders like this one.

### [Checkout the demo here](https://eshaz.github.io/wasm-audio-decoders/)

## Installing
* Install from [NPM](https://www.npmjs.com/package/@wasm-audio-decoders/flac).

  Run `npm i @wasm-audio-decoders/flac`

  ```javascript
  import { FLACDecoder } from '@wasm-audio-decoders/flac';

  const decoder = new FLACDecoder();
  ```
 
* Or download the [build](https://github.com/eshaz/wasm-audio-decoders/tree/main/src/flac/dist) and include it as a script.
  ```html
  <script src="flac-decoder.min.js" charset="UTF-8"></script>
  <script>
    const decoder = new window["flac-decoder"].FLACDecoder();
  </script>
  ```
* Note that the script must be read using UTF-8 character encoding. If running in a browser, the `<script>` tag must include the `charset="UTF-8"` attribute; or, the loading HTML must include `<meta charset="utf-8" />`.

## Usage

1. Create a new instance and wait for the WASM to finish compiling. Decoding can be done on the main thread synchronously, or in a web worker asynchronously.

   **Main thread synchronous decoding**
   ```javascript
   import { FLACDecoder } from '@wasm-audio-decoders/flac';

   const decoder = new FLACDecoder();

   // wait for the WASM to be compiled
   await decoder.ready;
   ```

   **Web Worker asynchronous decoding**
   ```javascript
   import { FLACDecoderWebWorker } from '@wasm-audio-decoders/flac';

   const decoder = new FLACDecoderWebWorker();

   // wait for the WASM to be compiled
   await decoder.ready;
   ```

1. Begin decoding FLAC or Ogg FLAC data.

   ```javascript  
   // Decode an individual Opus frame
   const {channelData, samplesDecoded, sampleRate, bitDepth} = decoder.decode(flacData);
   ```

1. When done decoding, reset the decoder to decode a new stream, or free up the memory being used by the WASM module if you have no more audio to decode. 

   ```javascript
   // `reset()` clears the decoder state and allows you do decode a new stream of FLAC data.
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
    bitDepth: 24, // bit depth of the original FLAC file
    errors: [ // array containing descriptions for any decode errors
      {
        message: "Error: FLAC__STREAM_DECODER_ERROR_STATUS_LOST_SYNC; State: FLAC__STREAM_DECODER_SEARCH_FOR_FRAME_SYNC",
        frameLength: 400, // length of the frame or data in bytes that encountered an error
        frameNumber: 5, // position of error relative to total frames decoded 
        inputBytes: 11606, // position of error relative to total input bytes
        outputSamples: 20480, // position of error relative to total output samples
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

See: https://datatracker.ietf.org/doc/html/rfc7845.html#section-5.1.1.2

## `FLACDecoder`

Class that decodes FLAC synchronously on the main thread.

### Options
```javascript
const decoder = new FLACDecoder();
```

### Getters
* `decoder.ready` *async*
  * Returns a promise that is resolved when the WASM is compiled and ready to use.

### Methods

* `decoder.decode(flacData)` *async*
  * `flacData` Uint8Array containing FLAC or Ogg FLAC data.
  * Returns a promise that resolves with the decoded audio.
  * Use this when streaming audio into the decoder.
* `decoder.flush()` *async*
  * Returns a promise that resolves with any remaining data in the buffer.
  * Use this when you are finished piping audio in through the `decode` method to retrieve any remaining data in the buffer.
* `decoder.decodeFile(flacData)` *async*
  * `flacData` Uint8Array containing FLAC or Ogg FLAC data.
  * Returns a promise that resolves with the decoded audio.
  * Use this when decoding an entire file.
* `decoder.decodeFrames(flacFrames)` *async*
  * `flacData` Array of Uint8Array containing FLAC frames.
  * Returns a promise that resolves with the decoded audio.
  * Use this when you already have the FLAC frames parsed and split into individual buffers.
* `decoder.reset()` *async*
  * Resets the decoder so that a new stream of FLAC data can be decoded.
* `decoder.free()`
  * De-allocates the memory used by the decoder.
  * After calling `free()`, the current instance is made unusable, and a new instance will need to be created to decode additional FLAC data.

## `FLACDecoderWebWorker`

Class that decodes FLAC asynchronously within a web worker. Decoding is performed in a separate, non-blocking thread. Each new instance spawns a new worker allowing you to run multiple workers for concurrent decoding of multiple streams.

### Options
```javascript
const decoder = new FLACDecoderWebWorker();
```

### Getters
* `decoder.ready` *async*
  * Returns a promise that is resolved when the WASM is compiled and ready to use.

### Methods

* `decoder.decode(flacData)` *async*
  * `flacData` Uint8Array containing FLAC or Ogg FLAC data.
  * Returns a promise that resolves with the decoded audio.
  * Use this when streaming audio into the decoder.
* `decoder.flush()` *async*
  * Returns a promise that resolves with any remaining data in the buffer.
  * Use this when you are finished piping audio in through the `decode` method to retrieve any remaining data in the buffer.
* `decoder.decodeFile(flacData)` *async*
  * `flacData` Uint8Array containing FLAC or Ogg FLAC data.
  * Returns a promise that resolves with the decoded audio.
  * Use this when decoding an entire file.
* `decoder.decodeFrames(flacFrames)` *async*
  * `flacData` Array of Uint8Array containing FLAC frames.
  * Returns a promise that resolves with the decoded audio.
  * Use this when you already have the FLAC frames parsed and split into individual buffers.
* `decoder.reset()` *async*
  * Resets the decoder so that a new stream of FLAC data can be decoded.
* `decoder.free()` *async
  * De-allocates the memory used by the decoder.
  * After calling `free()`, the current instance is made unusable, and a new instance will need to be created to decode additional FLAC data.

### Properly using the Web Worker interface

`FLACDecoderWebWorker` uses async functions to send operations to the web worker without blocking the main thread. To fully take advantage of the concurrency provided by web workers, your code should avoid using `await` on decode operations where it will block the main thread.

Each method call on a `FLACDecoderWebWorker` instance will queue up an operation to the web worker. Operations will complete within the web worker thread one at a time and in the same order in which the methods were called.

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

### Decoding multiple files using a **single** instance of `FLACDecoderWebWorker`

This example shows how to decode multiple files using a single `FLACDecoderWebWorker` instance. This code iterates over an array of input files (Array of Uint8Arrays) and queues up each file to be decoded one at a time.

First, wait for the decoder to become ready by calling `decoder.ready`.

For each iteration, `decode()` is called, it's result is pushed to the `decodedFiles` array, and `decoder.reset()` is called to prepare the decoder for a new file. These operations are queued up to the decoder instance and will complete one after another.

Finally, a call to `decoder.free()` is queued to clean up the memory stored by the decoder. This resolves when it and all of the other operations before it complete.

It's important to note that there is only one `await` operations in this example. Decoding can happen asynchronously and you only need to `await` when you need to use the results of the decode operation.

```javascript
  const inputFiles = [file1, file2, file3] // Array of Uint8Array file data

  const decoder = new FLACDecoderWebWorker();

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
### Decoding multiple files using **multiple** instances of `FLACDecoderWebWorker`

This example shows how to decode multiple files using multiple instances of `FLACDecoderWebWorker`. This code iterates over an array of input files (Array of Uint8Arrays) and spawns a new `FLACDecoderWebWorker` instance for each file and decodes the file. If you want to take full advantage of multi-core devices, this is the approach you will want to take since it will parallelize the decoding

For each input file, a new decoder is created, and the file is decoded using the `decode()` after  `decoder.ready` is resolved. The result of the `decode()` operation is returned, and a `finally()` function on the promise calls `decoder.free()` to free up the instance after the decode operations are completed.

Finally, `Promise.all()` wraps this array of promises and resolves when all decode operations are complete.

It's important to note that there is only one `await` operation in this example. Decoding can happen asynchronously and you only need to `await` when you need to use the results of the decode operation.

```javascript
  const inputFiles = [file1, file2, file3] // Array of Uint8Array file data

  // loops through each Uint8Array in `inputFiles` and decodes the files in separate threads
  const decodePromise = Promise.all(
    inputFiles.map((file) => {
      const decoder = new FLACDecoderWebWorker();

      return decoder.ready
        .then(() => decoder.decode(file)) // decode the input file
        .finally(() => decoder.free()); // free the decoder after resolving the decode result
    })
  );

  // do sync operations here

  // await when you need to have the all of the audio data decoded
  const decodedFiles = await decodePromise;
```