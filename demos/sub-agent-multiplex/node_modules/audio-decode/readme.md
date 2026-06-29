# audio-decode [![test](https://github.com/audiojs/audio-decode/actions/workflows/test.js.yml/badge.svg)](https://github.com/audiojs/audio-decode/actions/workflows/test.js.yml) [![stable](https://img.shields.io/badge/stability-unstable-green.svg)](http://github.com/badges/stability-badges)

Decode audio data from supported format to [AudioBuffer](https://github.com/audiojs/audio-buffer).

Supported formats:

* [x] `wav`
* [x] `mp3`
* [x] `ogg vorbis`
* [x] `flac`
* [x] `opus`
* [ ] `alac`
* [ ] `aac`
* [ ] `m4a`
* [x] [`qoa`](https://github.com/phoboslab/qoa)

[![npm install audio-decode](https://nodei.co/npm/audio-decode.png?mini=true)](https://npmjs.org/package/audio-decode/)

```js
import decodeAudio from 'audio-decode';
import buffer from 'audio-lena/mp3';

let audioBuffer = await decode(buffer);
```

`buffer` type can be: _ArrayBuffer_, _Uint8Array_ or _Buffer_.

`decode` is lazy: first call prepares decoder.

To get more granular control over individual decoders, use `decoders`:

```js
import decode, {decoders} from 'audio-decode';

await decoders.mp3(); // load & compile decoder
const audioBuffer = await decoders.mp3(mp3buf); // decode
```

## See also

* [wasm-audio-decoders](https://github.com/eshaz/wasm-audio-decoders) – best in class compact & fast WASM audio decoders.
* [Web Audio Decoders](https://developer.mozilla.org/en-US/docs/Web/API/AudioDecoder) – native decoders API, hope one day will be fixed or alternatively polyfilled.
* [decodeAudioData](https://github.com/eshaz/wasm-audio-decoders) – default in-browser decoding method.
* [ffmpeg.wasm](https://github.com/ffmpegwasm/ffmpeg.wasm) – ultimate encoding/decoding library (8.5Mb of code).

## License

[MIT](LICENSE)&nbsp;&nbsp;•&nbsp;&nbsp;<a href="https://github.com/krishnized/license/">🕉</a>

