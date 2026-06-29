export interface DecodeError {
  message: string;
  frameLength: number;
  frameNumber: number;
  inputBytes: number;
  outputSamples: number;
}
