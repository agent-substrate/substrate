import { BitInputStream } from "@thi.ng/bitstream";
import {
  qoa_lms_predict,
  qoa_lms_update,
  qoa_clamp,
  qoa_dequant_tab,
  LMS,
  QOA_MIN_FILESIZE,
  QOA_SLICE_LEN,
  QOA_MAGIC,
  QOA_LMS_LEN,
} from "./lib/common.js";

function decodeHeader(stream) {
  const magic = stream.read(32);
  if (magic !== QOA_MAGIC) {
    throw new Error(`Not a QOA file; expected magic number 'qoaf'`);
  }

  // peek first frame to get audio file data
  const header = {
    samples: stream.read(32),
    channels: stream.read(8),
    sampleRate: stream.read(24),
  };

  // go back to end of header
  stream.seek(64);

  // return data
  return header;
}

function qoa_decode_frame(stream, audio, lmses, channelData, sampleOffset) {
  const channels = stream.read(8);
  const sampleRate = stream.read(24);
  const samples = stream.read(16); // frame samples
  const frameSize = stream.read(16);

  const dataSize = Math.floor(frameSize - 8 - QOA_LMS_LEN * 4 * channels);
  const numSlices = Math.floor(dataSize / 8);
  const maxTotalSamples = numSlices * QOA_SLICE_LEN;

  if (
    channels != audio.channels ||
    sampleRate != audio.sampleRate ||
    samples * channels > maxTotalSamples
  ) {
    throw new Error(`invalid frame header data`);
  }

  // decode LMS history and weights
  for (let c = 0; c < channels; c++) {
    const lms = lmses[c];
    for (let i = 0; i < QOA_LMS_LEN; i++) {
      let h = stream.read(16);
      lms.history[i] = h;
    }
    for (let i = 0; i < QOA_LMS_LEN; i++) {
      let w = stream.read(16);
      lms.weights[i] = w;
    }
  }

  for (
    let sample_index = 0;
    sample_index < samples;
    sample_index += QOA_SLICE_LEN
  ) {
    for (let c = 0; c < channels; c++) {
      const scalefactor = stream.read(4);
      const table = qoa_dequant_tab[scalefactor];
      const slice_start = sample_index;
      const slice_end = Math.min(sample_index + QOA_SLICE_LEN, samples);
      const slice_count = slice_end - slice_start;
      const lms = lmses[c];
      const sampleData = channelData[c];
      let idx = sampleOffset + slice_start;
      const weights = lms.weights;
      const history = lms.history;
      let bitsRemaining = 60;
      // note: this loop is a hot code path and could be optimized
      for (let i = 0; i < slice_count; i++) {
        const predicted = qoa_lms_predict(weights, history);
        const quantized = stream.read(3);
        const dequantized = table[quantized];
        const reconstructed = qoa_clamp(predicted + dequantized, -32768, 32767);
        const sample =
          reconstructed < 0 ? reconstructed / 32768 : reconstructed / 32767;
        sampleData[idx++] = sample;
        qoa_lms_update(weights, history, reconstructed, dequantized);
        bitsRemaining -= 3;
      }
      // skip stream if needed
      if (bitsRemaining > 0) {
        stream.read(bitsRemaining);
      }
    }
  }

  return samples;
}

export default function decode(data) {
  if (data.byteLength < QOA_MIN_FILESIZE) {
    throw new Error(`QOA file size must be >= ${QOA_MIN_FILESIZE}`);
  }

  const stream = new BitInputStream(data);
  const audio = decodeHeader(stream);

  const channelData = [];
  const lmses = [];
  for (let c = 0; c < audio.channels; c++) {
    const d = new Float32Array(audio.samples);
    channelData.push(d);
    lmses.push(LMS());
  }

  let sampleIndex = 0;
  let frameLen = 0;
  do {
    frameLen = qoa_decode_frame(stream, audio, lmses, channelData, sampleIndex);
    sampleIndex += frameLen;
  } while (frameLen && sampleIndex < audio.samples);

  return {
    ...audio,
    channelData,
  };
}
