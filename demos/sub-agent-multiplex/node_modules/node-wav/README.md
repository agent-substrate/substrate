node-wav
========

High performance WAV decoder and encoder. The decoder is up to 750x faster than
the 'wav-decoder' npm module.

In addition, in contrast to 'wav-decoder' the result is returned directly instead
of with a promise.

This module combines the functionality of 'wav-decoder' and 'wav-encoder'.

Usage
-----

    let fs = require('fs');
    let wav = require('node-wav');

    let buffer = fs.readFileSync('file.wav');
    let result = wav.decode(buffer);
    console.log(result.sampleRate);
    console.log(result.channelData); // array of Float32Arrays

    wav.encode(result.channelData, { sampleRate: result.sampleRate, float: true, bitDepth: 32 });

Data format
-----------

Data is always returned as Float32Arrays. While reading and writing 64-bit float
WAV files is supported, data is truncated to 32-bit floats.

Endianness
----------

This module assumes a little endian CPU, which is true for pretty much every processor
these days (in particular Intel and ARM).