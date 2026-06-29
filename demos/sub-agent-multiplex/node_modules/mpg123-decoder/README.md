# `mpg123-decoder`

`mpg123-decoder` is a Web Assembly MPEG Layer (I/II/III) audio decoder.
  * 77.1 KiB minified bundle size
  * Browser and NodeJS support
  * Built in Web Worker support
  * Based on [`mpg123`](https://www.mpg123.de/)

See the [homepage](https://github.com/eshaz/wasm-audio-decoders) of this repository for more Web Assembly audio decoders like this one.

### [Checkout the demo here](https://eshaz.github.io/wasm-audio-decoders/)

## Installing
* Install from [NPM](https://www.npmjs.com/package/mpg123-decoder).
  
  Run `npm i mpg123-decoder`

  ```javascript
  import { MPEGDecoder } from 'mpg123-decoder';

  const decoder = new MPEGDecoder();
  ```
 
* Or download the [build](https://github.com/eshaz/wasm-audio-decoders/tree/main/src/mpg123-decoder/dist) and include it as a script.
  ```html
  <script src="mpg123-decoder.min.js" charset="UTF-8"></script>
  <script>
    const decoder = new window["mpg123-decoder"].MPEGDecoder();
  </script>
  ```
* Note that the script must be read using UTF-8 character encoding. If running in a browser, the `<script>` tag must include the `charset="UTF-8"` attribute; or, the loading HTML must include `<meta charset="utf-8" />`.

## Usage

1. Create a new instance and wait for the WASM to finish compiling. Decoding can be done on the main thread synchronously, or in a webworker asynchronously.
   
    **Main thread synchronous decoding**
    ```javascript
    import { MPEGDecoder } from 'mpg123-decoder';

    const decoder = new MPEGDecoder();

    // wait for the WASM to be compiled
    await decoder.ready;
    ```

    **Web Worker asynchronous decoding**
    ```javascript
    import { MPEGDecoderWebWorker } from 'mpg123-decoder';

    const decoder = new MPEGDecoderWebWorker();

    // wait for the WASM to be compiled
    await decoder.ready;
    ```

1. Begin decoding MPEG data.

   ```javascript
   // Decode Uint8Array of MPEG data
   const {channelData, samplesDecoded, sampleRate} = decoder.decode(mpegData);
   
   // Decode an individual MPEG frame
   const {channelData, samplesDecoded, sampleRate} = decoder.decodeFrame(mpegFrame);
   
   // Decode an array of individual MPEG frames
   const {channelData, samplesDecoded, sampleRate} = decoder.decodeFrames(mpegFrameArray);
   ```

1. When done decoding, reset the decoder to decode a new stream, or free up the memory being used by the WASM module if you have no more audio to decode. 

   ```javascript
   // `reset()` clears the decoder state and allows you do decode a new stream of MPEG data.
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
      rightAudio // Float32Array of PCM samples for the right channel
    ],
    samplesDecoded: 1234, // number of PCM samples that were decoded per channel
    sampleRate: 44100, // sample rate of the decoded PCM
    errors: [ // array containing descriptions for any decode errors
      {
        message: "-1 MPG123_ERR",
        frameLength: 1008, // length of the frame or data in bytes that encountered an error
        frameNumber: 0, // position of error relative to total frames decoded 
        inputBytes: 2160, // position of error relative to total input bytes
        outputSamples: 1152, // position of error relative to total output samples
      }
    ]
}
```

Each Float32Array within `channelData` can be used directly in the WebAudio API for playback.

Decoding will proceed through any errors. Any errors encountered may result in gaps in the decoded audio.

## `MPEGDecoder`

Class that decodes MPEG data or frames synchronously on the main thread.

### Options
```javascript
const decoder = new MPEGDecoder({ enableGapless: true });
```

* `enableGapless` *optional, defaults to `true`*
  * Set to `false` to disable gapless decoding.
  * Gapless decoding reads the XING / Lame header to determine delay (start) and padding (end) samples and trims the output accordingly.

### Getters
* `decoder.ready` *async*
  * Returns a promise that is resolved when the WASM is compiled and ready to use.

### Methods

* `decoder.decode(mpegData)`
  * `mpegData` Uint8Array of MPEG audio data.
* `decoder.decodeFrame(mpegFrame)`
  * `mpegFrame` Uint8Array containing a single MPEG frame.
* `decoder.decodeFrames(mpegFrames)`
  * `mpegFrames` Array of Uint8Arrays containing MPEG frames.
* `decoder.reset()` *async*
  * Resets the decoder so that a new stream of MPEG data can be decoded.
* `decoder.free()`
  * De-allocates the memory used by the decoder.
  * After calling `free()`, the current instance is made unusable, and a new instance will need to be created to decode additional MPEG data.

## `MPEGDecoderWebWorker`

Class that decodes MPEG data or frames asynchronously within a WebWorker. Decoding is performed in a separate, non-blocking thread. Each new instance spawns a new worker allowing you to run multiple workers for concurrent decoding of multiple streams.

### Options
```javascript
const decoder = new MPEGDecoderWebWorker({ enableGapless: true });
```

* `enableGapless` *optional, defaults to `true`*
  * Set to `false` to disable gapless decoding.
  * Gapless decoding reads the XING / Lame header to determine delay (start) and padding (end) samples and trims the output accordingly.

### Getters
* `decoder.ready` *async*
  * Returns a promise that is resolved when the WASM is compiled and ready to use.

### Methods

* `decoder.decode(mpegData)` *async*
  * `mpegData` Uint8Array of MPEG audio data.
* `decoder.decodeFrame(mpegFrame)` *async*
  * `mpegFrame` Uint8Array containing a single MPEG frame.
* `decoder.decodeFrames(mpegFrames)` *async*
  * `mpegFrames` Array of Uint8Arrays containing MPEG frames.
* `decoder.reset()` *async*
  * Resets the decoder so that a new stream of MPEG data can be decoded.
* `decoder.free()` *async*
  * De-allocates the memory used by the decoder and terminates the web worker.
  * After calling `free()`, the current instance is made unusable, and a new instance will need to be created to decode additional MPEG data.

### Properly using the Web Worker interface

`MPEGDecoderWebWorker` uses async functions to send operations to the web worker without blocking the main thread. To fully take advantage of the concurrency provided by web workers, your code should avoid using `await` on decode operations where it will block the main thread.

Each method call on a `MPEGDecoderWebWorker` instance will queue up an operation to the web worker. Operations will complete within the web worker thread one at a time and in the same order in which the methods were called.

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

## Examples

### Decoding multiple files using a **single** instance of `MPEGDecoderWebWorker`

This example shows how to decode multiple files using a single `MPEGDecoderWebWorker` instance. This code iterates over an array of input files (Array of Uint8Arrays) and queues up each file to be decoded one at a time.

First, wait for the decoder to become ready by calling `decoder.ready`.

For each iteration, `decode()` is called, it's result is pushed to the `decodedFiles` array, and `decoder.reset()` is called to prepare the decoder for a new file. These operations are queued up to the decoder instance and will complete one after another.

Finally, a call to `decoder.free()` is queued to clean up the memory stored by the decoder. This resolves when it and all of the other operations before it complete.

It's important to note that there is only one `await` operations in this example. Decoding can happen asynchronously and you only need to `await` when you need to use the results of the decode operation.

```javascript
  const inputFiles = [file1, file2, file3] // Array of Uint8Array file data

  const decoder = new MPEGDecoderWebWorker();

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
### Decoding multiple files using **multiple** instances of `MPEGDecoderWebWorker`

This example shows how to decode multiple files using multiple instances of `MPEGDecoderWebWorker`. This code iterates over an array of input files (Array of Uint8Arrays) and spawns a new `MPEGDecoderWebWorker` instance for each file and decodes the file. If you want to take full advantage of multi-core devices, this is the approach you will want to take since it will parallelize the decoding

For each input file, a new decoder is created, and the file is decoded using the `decode()` after  `decoder.ready` is resolved. The result of the `decode()` operation is returned, and a `finally()` function on the promise calls `decoder.free()` to free up the instance after the decode operations are completed.

Finally, `Promise.all()` wraps this array of promises and resolves when all decode operations are complete.

It's important to note that there is only one `await` operation in this example. Decoding can happen asynchronously and you only need to `await` when you need to use the results of the decode operation.

```javascript
  const inputFiles = [file1, file2, file3] // Array of Uint8Array file data

  // loops through each Uint8Array in `inputFiles` and decodes the files in separate threads
  const decodePromise = Promise.all(
    inputFiles.map((file) => {
      const decoder = new MPEGDecoderWebWorker();

      return decoder.ready
        .then(() => decoder.decode(file)) // decode the input file
        .finally(() => decoder.free()); // free the decoder after resolving the decode result
    })
  );

  // do sync operations here

  // await when you need to have the all of the audio data decoded
  const decodedFiles = await decodePromise;
```
