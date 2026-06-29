/* -*- Mode: Java; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set shiftwidth=2 tabstop=2 autoindent cindent expandtab: */

"use strict";

const fs = require('fs');
const encoder = require('wav-encoder');
const decoder = require('wav-decoder');
const wav = require('../index');

function make_test_data(channels, samples) {
  let data = [];
  for (let ch = 0; ch < channels; ++ch) {
    data[ch] = new Float32Array(samples);
    for (let n = 0; n < samples; ++n)
      data[ch][n] = Math.random();
  }
  return data;
}

exports.test_wav = test => {
  test.expect(25);
  let samples = 10;
  let channels = 2;
  let data = make_test_data(channels, samples);
  Promise.all([8, 16, 24, 32, '32f'].map(bitDepth => new Promise((resolve, reject) => {
    let floatingPoint = false;
    if (bitDepth == '32f') {
      bitDepth = 32;
      floatingPoint = true;
    }
    let audioData = {
      length: samples,
      numberOfChannels: channels,
      sampleRate: 16000,
      channelData: data
    };
    let opts = {
      floatingPoint: floatingPoint,
      bitDepth: bitDepth,
    };
    encoder.encode(audioData, opts).then(buffer => {
      let encoded = wav.encode(audioData.channelData, opts);
      test.equal(new Buffer(buffer).toString('hex'), encoded.toString('hex'), 'our encoder should match wav-encoder');
      let decoded = wav.decode(buffer);
      decoder.decode(buffer).then(reference => {
        test.equal(reference.length, decoded.channelData[0].length, 'number of samples should match');
        test.equal(reference.numberOfChannels, decoded.channelData.length, 'number of channels should match');
        test.equal(reference.sampleRate, decoded.sampleRate, 'sample rate should match');
        test.deepEqual(reference.channelData, decoded.channelData, 'data should match');
        resolve();
      });
    });
  }))).then(() => test.done());
};

exports.test_buffer_offset = test => {
  test.expect(1);
  let files = ["./tests/file1.wav"];
  files.forEach(file => {
    let buffer = fs.readFileSync(file);
    let decoded = wav.decode(buffer);
    test.equal(decoded.sampleRate, 16000);
    test.done();
  });
};
