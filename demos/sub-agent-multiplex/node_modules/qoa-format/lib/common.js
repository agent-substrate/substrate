export const QOA_MIN_FILESIZE = 16;
export const QOA_MAX_CHANNELS = 8;

export const QOA_SLICE_LEN = 20;
export const QOA_SLICES_PER_FRAME = 256;
export const QOA_FRAME_LEN = QOA_SLICES_PER_FRAME * QOA_SLICE_LEN;
export const QOA_LMS_LEN = 4;
export const QOA_MAGIC = 0x716f6166; /* 'qoaf' */
export const QOA_FRAME_SIZE = (channels, slices) =>
  Math.floor(8 + QOA_LMS_LEN * 4 * channels + 8 * slices * channels);

export function qoa_clamp(v, min, max) {
  return v < min ? min : v > max ? max : v;
}

export function LMS(h, w) {
  const history = new Int16Array(h || 4);
  const weights = new Int16Array(w || 4);
  return { history, weights };
}

export function qoa_lms_predict(weights, history) {
  return (
    (weights[0] * history[0] +
      weights[1] * history[1] +
      weights[2] * history[2] +
      weights[3] * history[3]) >>
    13
  );
}

export function qoa_lms_update(weights, history, sample, residual) {
  let delta = residual >> 4;
  weights[0] += history[0] < 0 ? -delta : delta;
  weights[1] += history[1] < 0 ? -delta : delta;
  weights[2] += history[2] < 0 ? -delta : delta;
  weights[3] += history[3] < 0 ? -delta : delta;
  history[0] = history[1];
  history[1] = history[2];
  history[2] = history[3];
  history[3] = sample;
}

export const qoa_round = (num) => Math.sign(num) * Math.round(Math.abs(num));

/* We have 16 different scalefactors. Like the quantized residuals these become
less accurate at the higher end. In theory, the highest scalefactor that we
would need to encode the highest 16bit residual is (2**16)/8 = 8192. However we
rely on the LMS filter to predict samples accurately enough that a maximum 
residual of one quarter of the 16 bit range is high sufficent. I.e. with the 
scalefactor 2048 times the quant range of 8 we can encode residuals up to 2**14.

The scalefactor values are computed as:
scalefactor_tab[s] <- round(pow(s + 1, 2.75)) */

export const qoa_scalefactor_tab = Array(16)
  .fill()
  .map((_, s) => qoa_round(Math.pow(s + 1, 2.75)));

/* The dequant_tab maps each of the scalefactors and quantized residuals to 
their unscaled & dequantized version.

Since qoa_div rounds away from the zero, the smallest entries are mapped to 3/4
instead of 1. The dequant_tab assumes the following dequantized values for each 
of the quant_tab indices and is computed as:
float dqt[8] = {0.75, -0.75, 2.5, -2.5, 4.5, -4.5, 7, -7};
dequant_tab[s][q] <- round(scalefactor_tab[s] * dqt[q]) */

const dqt = [0.75, -0.75, 2.5, -2.5, 4.5, -4.5, 7, -7];
export const qoa_dequant_tab = qoa_scalefactor_tab.map((sf) => {
  return dqt.map((dq) => qoa_round(dq * sf));
});
