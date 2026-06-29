import { BitOutputStream } from "@thi.ng/bitstream";
import {
  qoa_lms_predict,
  qoa_lms_update,
  qoa_clamp,
  qoa_dequant_tab,
  qoa_scalefactor_tab,
  LMS,
  QOA_SLICE_LEN,
  QOA_FRAME_LEN,
  QOA_MAGIC,
  QOA_LMS_LEN,
  QOA_FRAME_SIZE,
} from "./lib/common.js";

/* The reciprocal_tab maps each of the 16 scalefactors to their rounded 
reciprocals 1/scalefactor. This allows us to calculate the scaled residuals in 
the encoder with just one multiplication instead of an expensive division. We 
do this in .16 fixed point with integers, instead of floats.

The reciprocal_tab is computed as:
reciprocal_tab[s] <- ((1<<16) + scalefactor_tab[s] - 1) / scalefactor_tab[s] */

const qoa_reciprocal_tab = qoa_scalefactor_tab.map((s) =>
  Math.floor(((1 << 16) + s - 1) / s)
);

/* The quant_tab provides an index into the dequant_tab for residuals in the
range of -8 .. 8. It maps this range to just 3bits and becommes less accurate at 
the higher end. Note that the residual zero is identical to the lowest positive 
value. This is mostly fine, since the qoa_div() function always rounds away 
from zero. */

const qoa_quant_tab = [
  // -8..-1
  7, 7, 7, 5, 5, 3, 3, 1,
  // 0
  0,
  //  1.. 8
  0, 2, 2, 4, 4, 6, 6, 6,
];

/* qoa_div() implements a rounding division, but avoids rounding to zero for 
small numbers. E.g. 0.1 will be rounded to 1. Note that 0 itself still 
returns as 0, which is handled in the qoa_quant_tab[].
qoa_div() takes an index into the .16 fixed point qoa_reciprocal_tab as an
argument, so it can do the division with a cheaper integer multiplication. */

function qoa_div(v, scalefactor) {
  const reciprocal = qoa_reciprocal_tab[scalefactor];
  let n = (v * reciprocal + (1 << 15)) >> 16;
  n = n + ((v > 0) - (v < 0)) - ((n > 0) - (n < 0)); /* round away from 0 */
  return n;
}

function qoa_encode_frame(stream, audio, lmses, sample_offset, frame_len) {
  const channels = audio.channels;
  const sampleRate = audio.sampleRate;
  const channelData = audio.channelData;
  const samples = audio.samples;

  const slices = Math.floor((frame_len + QOA_SLICE_LEN - 1) / QOA_SLICE_LEN);
  const frame_size = QOA_FRAME_SIZE(channels, slices);

  // Frame header
  stream.write(channels, 8);
  stream.write(sampleRate, 24);
  stream.write(frame_len, 16); // frame samples
  stream.write(frame_size, 16);

  // write current LMS weights and history state
  for (let c = 0; c < channels; c++) {
    const lms = lmses[c];

    /* If the weights have grown too large, reset them to 0. This may happen
		with certain high-frequency sounds. This is a last resort and will 
		introduce quite a bit of noise, but should at least prevent pops/clicks */
    const weights_sum =
      lms.weights[0] * lms.weights[0] +
      lms.weights[1] * lms.weights[1] +
      lms.weights[2] * lms.weights[2] +
      lms.weights[3] * lms.weights[3];
    if (weights_sum > 0x2fffffff) {
      lms.weights[0] = 0;
      lms.weights[1] = 0;
      lms.weights[2] = 0;
      lms.weights[3] = 0;
    }

    for (let i = 0; i < QOA_LMS_LEN; i++) {
      stream.write(lms.history[i], 16);
    }
    for (let i = 0; i < QOA_LMS_LEN; i++) {
      stream.write(lms.weights[i], 16);
    }
  }

  /* We encode all samples with the channels interleaved on a slice level.
	E.g. for stereo: (ch-0, slice 0), (ch 1, slice 0), (ch 0, slice 1), ...*/
  for (
    let sample_index = 0;
    sample_index < frame_len;
    sample_index += QOA_SLICE_LEN
  ) {
    for (let c = 0; c < channels; c++) {
      const slice_len = qoa_clamp(QOA_SLICE_LEN, 0, frame_len - sample_index);
      const slice_start = sample_index;

      /* Brute for search for the best scalefactor. Just go through all
			16 scalefactors, encode all samples for the current slice and 
			meassure the total squared error. */
      let best_error = Number.MAX_SAFE_INTEGER;
      let best_slice;
      let best_slice_scalefactor;
      let best_lms;
      const sampleData = channelData[c];

      for (let scalefactor = 0; scalefactor < 16; scalefactor++) {
        /* We have to reset the LMS state to the last known good one
				before trying each scalefactor, as each pass updates the LMS
				state when encoding. */
        let lms = LMS(lmses[c].history, lmses[c].weights);

        const table = qoa_dequant_tab[scalefactor];

        // an array of slice data
        let slice = [];
        let current_error = 0;
        let idx = slice_start + sample_offset;

        for (let i = 0; i < slice_len; i++) {
          let sample = sampleData[idx++];

          // turn into 16 bit signed integer
          sample = Math.floor(
            Math.fround(sample < 0 ? sample * 32768 : sample * 32767)
          );
          sample = qoa_clamp(sample, -32768, 32767);

          let predicted = qoa_lms_predict(lms.weights, lms.history);
          let residual = sample - predicted;
          let scaled = qoa_div(residual, scalefactor);
          let clamped = qoa_clamp(scaled, -8, 8);
          let quantized = qoa_quant_tab[clamped + 8];
          let dequantized = table[quantized];
          let reconstructed = qoa_clamp(predicted + dequantized, -32768, 32767);
          let error = sample - reconstructed;
          current_error += error * error;
          if (current_error > best_error) {
            break;
          }

          qoa_lms_update(lms.weights, lms.history, reconstructed, dequantized);
          slice.push(quantized);
        }

        if (current_error < best_error) {
          best_error = current_error;
          best_slice = slice;
          best_slice_scalefactor = scalefactor;
          best_lms = lms;
        }
      }

      lmses[c] = best_lms;
      // first, write the 4bit scalefactor
      stream.write(best_slice_scalefactor, 4);
      // now write each 3bit datum in the slice
      for (let i = 0; i < QOA_SLICE_LEN; i++) {
        // the last frame of a file might be smaller than QOA_SLICE_LEN
        const v = i < best_slice.length ? best_slice[i] : 0;
        stream.write(v, 3);
      }
    }
  }
}

export default function encode({ channelData, sampleRate = 44100 } = {}) {
  const channels = channelData.length;
  const samples = channels >= 1 ? channelData[0].length : 0;
  const audio = {
    samples,
    channels,
    channelData,
    sampleRate,
  };

  const num_frames = (samples + QOA_FRAME_LEN - 1) / QOA_FRAME_LEN;
  const num_slices = (samples + QOA_SLICE_LEN - 1) / QOA_SLICE_LEN;
  const encoded_size =
    8 /* 8 byte file header */ +
    num_frames * 8 /* 8 byte frame headers */ +
    num_frames *
      QOA_LMS_LEN *
      4 *
      audio.channels /* 4 * 4 bytes lms state per channel */ +
    num_slices * 8 * audio.channels; /* 8 byte slices */

  const lmses = [];
  for (let c = 0; c < audio.channels; c++) {
    const lms = LMS();
    lms.weights[0] = 0;
    lms.weights[1] = 0;
    lms.weights[2] = -(1 << 13);
    lms.weights[3] = 1 << 14;
    lmses.push(lms);
  }

  // write header
  const stream = new BitOutputStream(encoded_size);
  stream.write(QOA_MAGIC, 32);
  stream.write(samples, 32);

  let frame_len = QOA_FRAME_LEN;
  for (
    let sample_index = 0;
    sample_index < samples;
    sample_index += frame_len
  ) {
    frame_len = qoa_clamp(QOA_FRAME_LEN, 0, samples - sample_index);
    qoa_encode_frame(stream, audio, lmses, sample_index, frame_len);
  }

  return stream.bytes();
}
