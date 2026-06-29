# Codec Parser

`codec-parser` is a JavaScript library that parses raw data from audio codecs into frames containing data, header values, duration, and other information.

### Supports:
  * **MPEG Layer I/II/III (MP3)** - `audio/mpeg`
  * **AAC** - `audio/aac`, `audio/aacp`
  * **FLAC** - `audio/flac`
  * **Ogg FLAC** - `application/ogg`, `audio/ogg`
  * **Ogg Opus** - `application/ogg`, `audio/ogg`
  * **Ogg Vorbis** - `application/ogg`, `audio/ogg`

## Demo
The demo for [`icecast-metadata-js`](https://github.com/eshaz/icecast-metadata-js) uses this library to allow for playback of streaming audio. `codec-parser` is used by [`mse-audio-wrapper`](https://github.com/eshaz/mse-audio-wrapper) to wrap streaming audio in ISOBMFF or WEBM so it can be played back using the MediaSource API.

## View the live demo [here](https://eshaz.github.io/icecast-metadata-js/)!

---

* [Installing](#installing)
* [Usage](#usage)
  * [Instantiation](#instantiation)
  * [Methods](#methods)
  * [Properties](#properties)
* [Data Types](#data-types)
  * [OggPage](#oggpage)
  * [CodecFrame](#codecframe)
  * [CodecHeader](#codecheader)
    * [MPEGHeader](#mpegheader)
    * [AACHeader](#aacheader)
    * [FLACHeader](#flacheader)
    * [OpusHeader](#opusheader)
    * [VorbisHeader](#vorbisheader)

## Installing

### Install via [NPM](https://www.npmjs.com/package/codec-parser)
* `npm i codec-parser`

## Usage

1. Create a new instance of `CodecParser` by passing in the mimetype of your audio data along with the options object.

    *Note: For directly reading from a HTTP response, use the mimetype contained in the `Content-Type` header*
    
    ```javascript
    import CodecParser from "codec-parser";

    const mimeType = "audio/mpeg";
    const options = {
        onCodec: () => {},
        onCodecUpdate: () => {},
        enableLogging: true
    };

    const parser = new CodecParser(mimeType, options);
    ```

### Parsing an entire file

1. To parse an entire audio file, pass in a Uint8Array of the entire audio file into the instance's `.parseAll()`. This method will read the all of the data and return an array of [`CodecFrame`](#codecframe)s or [`OggPage`](#oggpage)s.

    ```javascript
    const frames = parser.parseAll(audioData);

    // Do something with the frames
    ```

### Parsing chunks of audio

1. To begin processing chunks of audio data, pass in a Uint8Array of audio data into the instance's `.parseChunk()`. This method returns an iterator that can be consumed using a `for ...of` or `for await...of` loop.

    ```javascript
    for (const frame of parser.parseChunk(audioData)) {
      // Do something with each frame
    }
    ```
    ***or***
    ```javascript
    const frames = [...parser.parseChunk(audioData)]
    ```

    `CodecParser` will read the passed in data and attempt to parse audio frames according to the passed in `mimeType`. Any partial data will be stored until enough data is passed in for a complete frame can be formed. Iterations will begin to return frames once at least two consecutive frames have been detected in the passed in data.

    *Note: Any data that does not conform to the instance's mimetype will be discarded.*

    ### Example:

    * 1st `.parseChunk()` call
      * Input
          ```
          [MPEG frame 0 (partial)],
          [MPEG frame 1 (partial)], 
          ```
      * Output (no iterations)
        ```
        (none)
        ```
      * `Frame 0` is dropped since it doesn't start with a valid header.
      * `Frame 1` is parsed and stored internally until enough data is passed in to properly sync.
    * 2nd `.parseChunk()` call
      * Input
          ```
          [MPEG frame 1 (partial)], 
          [MPEG frame 2 (partial)]
          ```
      * Output (1 iteration)
        ```
        MPEG Frame 1 {
            data,
            header
            ...
        }
        ```
      * `Frame 1` is joined with the partial data and returned since it was immediately followed by `Frame 2`.
      * `Frame 2` is stored internally as partial data.
    * 3rd `.parseChunk()` call
      * Input
        ```
        [MPEG frame 2 (partial)],
        [MPEG frame 3 (full)], 
        [MPEG frame 4 (partial)]
        ```
      * Output (2 iterations)
        ```
        MPEG Frame 2 {
            data,
            header
            ...
        }
        ```
        ```
        MPEG Frame 3 {
            data,
            header
            ...
        }
        ```
      * `Frame 2` is joined with the partial data and returned since it was immediately followed by `Frame 3`.
      * `Frame 3` is returned since it was immediately followed by `Frame 4`.
      * `Frame 4` is stored internally as partial data.

1. When you have come to the end of the stream or file, you may call the instance's `flush()` method to return another iterator that will yield any remaining frames that are buffered. Calling `flush()` will reset the internal state of the `CodecParser` instance and may re-use the instance to parse additional streams.

    ```javascript
    for (const frame of parser.flush()) {
      // Do something the buffered frames
    }
    ```
    ***or***
    ```javascript
    const frames = [...parser.flush()]
    ```


### Instantiation

`const parser = new CodecParser("audio/mpeg", options);`
* `constructor` Creates a new instance of CodecParser that can be used to parse audio for a given mimetype.
  * `mimetype` *required* Incoming audio codec or container
    * MP3 - `audio/mpeg`
    * AAC - `audio/aac`, `audio/aacp`
    * FLAC - `audio/flac`
    * Ogg FLAC - `application/ogg`, `audio/ogg`
    * Ogg Opus - `application/ogg`, `audio/ogg`
    * Ogg Vorbis - `application/ogg`, `audio/ogg`
  * `options` *optional*
    * `options.onCodec()` *optional* Called when the output codec is determined.
      * See `parser.codec` for a list of the possible output codecs
    * `options.onCodecHeader(codecHeaderData)` *optional* Called once when the codec header is first parsed.
      * `codecHeaderData` Object containing codec header information.
    * `options.onCodecUpdate(codecHeaderData, updateTimestamp)` *optional* Called when there is a change in the codec header.
      * `codecHeaderData` Object containing codec header information that was updated.
      * `updateTimestamp` Timestamp in milliseconds when the codec information was updated.
    * `options.enableLogging` *optional* Set to true to enable warning and error messages.
    * `options.enableFrameCRC32` *optional* Set to false to disable the crc32 calculation for each frame. This will save a marginal amount of execution time if you don't need this information.

### Methods

* `parser.parseAll(data)` Function that takes a audio data for an entire file.
  * `data` `Uint8Array` of audio data for a complete audio stream / file
  * Returns an Array of [`CodecFrame`](#codecframe)s or [`OggPage`](#oggpage)s for the entire file
* `parser.parseChunk(chunk)` Generator function that yields frames for a partial chunk of audio data from an audio stream or file
  * `chunk` `Uint8Array` of audio data
  * Returns `Iterator` that yields a parsed [`CodecFrame`](#codecframe) or [`OggPage`](#oggpage) for each iteration.
* `parser.flush()` Generator function that yields any buffered frames that are stored after `parseChunk()` completes
  * Returns `Iterator` that yields a parsed [`CodecFrame`](#codecframe) or [`OggPage`](#oggpage) for each iteration.
  * This function can be used after `parseChunk` has been called with all of the audio data you intend to parse. The final iterator returned by `parseChunk()` must be consumed before calling `flush()`.
  * Calling `flush()` will reset the internal state of the `CodecParser` instance. You may re-use the instance to parse additional streams.

### Properties

* `parser.codec` The detected codec of the audio data
    * **Note: For Ogg streams, the codec will only be available after Ogg identification header has been parsed.**
  * Values:
    * MPEG (MP3) - `"mpeg"`
    * AAC - `"aac"`
    * FLAC - `"flac"`
    * Opus - `"opus"`
    * Vorbis - `"vorbis"`

## Data Types

Depending on the mimetype each iteration of `CodecParser.parseChunk()` will return a single `CodecFrame` or a single `OggPage`.

### `OggPage`

`OggPage` describes a single ogg page. An `OggPage` may contain zero to many `CodecFrame` objects. `OggPage` will be returned when the mimetype is `audio/ogg` or `application/ogg`.

* `absoluteGranulePosition`: Total audio samples in the ogg stream up to the end of this `OggPage`.
* `codecFrames`: Array of `CodecFrame`(s) contained within this `OggPage`.
* `crc32`: CRC-32 hash of the frame data using the Ogg formula / polynomial.
* `data`: `Uint8Array` containing the page segments within the ogg page.
* `duration`: Audio duration in milliseconds contained within this `OggPage`.
* `isContinuedPacket`: Boolean indicating if this `OggPage` is part of a continued packet.
* `isFirstPage`: Boolean indicating if this `OggPage` is the first page in the Ogg stream.
* `isLastPage`: Boolean indicating if this this `OggPage` is the final page in the Ogg stream.
* `pageSequenceNumber`: Page sequence number within the Ogg stream.
* `rawData`: `Uint8Array` Total data of the `OggPage`.
* `samples`: Total number of audio samples contained within the `OggPage`.
* `streamSerialNumber`: Serial number of the Ogg stream.
* `totalBytesOut`: Total bytes of codec data output by `CodecParer` at the end of this ogg page.
* `totalDuration`: Total audio samples output by `CodecParer` at the end of this ogg page.
* `totalSamples`: Total audio duration in milliseconds output by `CodecParer` at the end of this ogg page.

### `CodecFrame`

`CodecFrame` describes a single frame for an audio codec. `CodecFrame` will be returned when the mimetype describes audio that is not encapsulated within a container i.e. `audio/mpeg`, `audio/aac`, or `audio/flac`.

* `data`: `Uint8Array` containing the audio data within this frame.
* `header`: [`Header`](#header) object describing the codec information.
* `crc32`: CRC-32 hash of the frame data.
* `samples`: Audio samples contained within this frame.
* `duration`: Audio duration in milliseconds contained within this frame.
* `frameNumber`: Total count of frames output by `CodecParser` starting at 0.
* `totalBytesOut`: Total bytes output by `CodecParer` not including this frame.
* `totalSamples`: Total audio samples output by `CodecParer` not including this frame.
* `totalDuration`: Total audio duration in milliseconds output by `CodecParer` not including this frame.

#### Example
```javascript
// First CodecFrame
MPEGFrame {
  data: Uint8Array(417),
  header: MPEGHeader {
    bitDepth: 16,
    channels: 2,
    sampleRate: 44100,
    bitrate: 128,
    channelMode: "joint stereo",
    emphasis: "none",
    framePadding: 1,
    isCopyrighted: false,
    isOriginal: true,
    isPrivate: false,
    layer: "Layer III",
    modeExtension: "Intensity stereo off, MS stereo on",
    mpegVersion: "MPEG Version 1 (ISO/IEC 11172-3)",
    protection: "none"
  },
  crc32: 275944052,
  samples: 1152,
  duration: 26.122448979591837,
  frameNumber: 0,
  totalBytesOut: 0,
  totalSamples: 0,
  totalDuration: 0
}

// Second CodecFrame
MPEGFrame {
  data: Uint8Array(416),
  header: MPEGHeader {
    bitDepth: 16,
    channels: 2,
    sampleRate: 44100,
    bitrate: 128,
    channelMode: "joint stereo",
    emphasis: "none",
    framePadding: 0,
    isCopyrighted: false,
    isOriginal: true,
    isPrivate: false,
    layer: "Layer III",
    modeExtension: "Intensity stereo off, MS stereo on",
    mpegVersion: "MPEG Version 1 (ISO/IEC 11172-3)",
    protection: "none"
  },
  crc32: 1336875295,
  samples: 1152,
  duration: 26.122448979591837,
  frameNumber: 1,
  totalBytesOut: 418,
  totalSamples: 1152,
  totalDuration: 26.122448979591837
}
```

### CodecHeader

Each codec has it's own `CodecHeader` data type. See each class below for documentation on each codec specific header.

### `MPEGHeader`
[***Documentation***](https://github.com/eshaz/codec-parser/blob/master/src/codecs/mpeg/MPEGHeader.js)
```javascript
{
  bitDepth: 16,
  bitrate: 192,
  channels: 2,
  sampleRate: 44100,
  channelMode: "joint stereo",
  emphasis: "none",
  framePadding: 1,
  isCopyrighted: false,
  isOriginal: false,
  isPrivate: false,
  layer: "Layer III",
  modeExtension: "Intensity stereo off, MS stereo on",
  mpegVersion: "MPEG Version 1 (ISO/IEC 11172-3)",
  protection: "16bit CRC"
}
```
### `AACHeader`
[***Documentation***](https://github.com/eshaz/codec-parser/blob/master/src/codecs/aac/AACHeader.js)
```javascript
{
  bitDepth: 16,
  bitrate: 312,
  channels: 2,
  sampleRate: 44100,
  copyrightId: false,
  copyrightIdStart: false,
  channelMode: "stereo (left, right)",
  bufferFullness: "VBR",
  isHome: false,
  isOriginal: false,
  isPrivate: false,
  layer: "valid",
  length: 7,
  mpegVersion: "MPEG-4",
  numberAACFrames: 0,
  profile: "AAC LC (Low Complexity)",
  protection: "none"
}
```

### `FLACHeader`
[***Documentation***](https://github.com/eshaz/codec-parser/blob/master/src/codecs/flac/FLACHeader.js)
```javascript
{
  bitDepth: 16,
  bitrate: 400,
  channels: 2,
  sampleRate: 44100,
  channelMode: "stereo (left, right)",
  blockingStrategy: "Fixed",
  blockSize: 4096,
  frameNumber: 15183508,
  crc16: 56624,
  streamInfo: Uint8Array
}
```

### `OpusHeader`
[***Documentation***](https://github.com/eshaz/codec-parser/blob/master/src/codecs/opus/OpusHeader.js)
```javascript
{
  bitDepth: 16,
  bitrate: 192,
  channels: 2,
  data: Uint8Array,
  sampleRate: 48000,
  bandwidth: "fullband",
  channelMappingFamily: 1,
  channelMappingTable: [0, 1],
  coupledStreamCount: 1,
  streamCount: 1,
  channelMode: "stereo (left, right)",
  frameCount: 1,
  frameSize: 20,
  inputSampleRate: 48000,
  mode: "CELT-only",
  outputGain: 0,
  preSkip: 312
}
```
### `VorbisHeader`
[***Documentation***](https://github.com/eshaz/codec-parser/blob/master/src/codecs/vorbis/VorbisHeader.js)
```javascript
{
  bitDepth: 32,
  bitrate: 272,
  channels: 2,
  channelMode: "stereo (left, right)",
  sampleRate: 44100,
  bitrateMaximum: 0,
  bitrateMinimum: 0,
  bitrateNominal: 320000,
  blocksize0: 256,
  blocksize1: 2048
  data: Uint8Array,
  vorbisComments: Uint8Array,
  vorbisSetup: Uint8Array
}
```