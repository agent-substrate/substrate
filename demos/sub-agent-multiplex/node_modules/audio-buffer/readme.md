# audio-buffer [![test](https://github.com/audiojs/audio-buffer/actions/workflows/node.js.yml/badge.svg)](https://github.com/audiojs/audio-buffer/actions/workflows/node.js.yml) [![stable](https://img.shields.io/badge/stability-stable-brightgreen.svg)](http://github.com/badges/stability-badges)

_AudioBuffer_ - basic audio data container class.
Useful instead of _Buffer_ in audio streams, [**@audiojs**](https://github.com/audiojs) components, in webworkers, nodejs, other environments without audio context.

Implementation is compatible with [Web Audio API AudioBuffer](https://developer.mozilla.org/en-US/docs/Web/API/AudioBuffer), can be used as ponyfill.

## Usage

[![npm install audio-buffer](https://nodei.co/npm/audio-buffer.png?mini=true)](https://npmjs.org/package/audio-buffer/)

### new AudioBuffer(options)

Create audio buffer from `options`.

* `options.length` — number of samples, minimum is 1.
* `options.sampleRate` — default sample rate is 44100.
* `options.numberOfChannels` — default number of channels is 1.

### buffer.duration

Duration of the underlying audio data, in seconds.

### buffer.length

Number of samples per channel.

### buffer.sampleRate

Default sample rate is 44100.

### buffer.numberOfChannels

Default number of channels is 1.

### buffer.getChannelData(channel)

Get array containing the data for the channel (not copied).

### buffer.copyFromChannel(destination, channelNumber, startInChannel=0)

Place data from channel to destination Float32Array.

### buffer.copyToChannel(source, channelNumber, startInChannel=0)

Place data from source Float32Array to the channel.


## Similar

* [ndsamples](https://github.com/livejs/ndsamples) — audio-wrapper for ndarrays. A somewhat alternative approach to wrap audio data, based on ndarrays, used by some modules in [livejs](https://github.com/livejs).
* [1](https://www.npmjs.com/package/audiobuffer), [2](https://www.npmjs.com/package/audio-buffer), [3](https://github.com/sebpiq/node-web-audio-api/blob/master/lib/AudioBuffer.js), [4](https://developer.mozilla.org/en-US/docs/Web/API/AudioBuffer) — other AudioBuffer implementations.
* [audiodata](https://www.npmjs.com/package/audiodata) alternative data holder from @mohayonao.


<p align=center><a href="https://github.com/krishnized/license/">🕉</a></p>
