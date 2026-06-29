import { defError } from "./deferror.js";
const UnsupportedOperationError = defError(
  () => "unsupported operation"
);
const unsupportedOp = (msg) => {
  throw new UnsupportedOperationError(msg);
};
const unsupported = unsupportedOp;
const UnsupportedFeatureError = defError(
  () => "unsupported feature"
);
const unsupportedFeature = (msg) => {
  throw new UnsupportedFeatureError(msg);
};
export {
  UnsupportedFeatureError,
  UnsupportedOperationError,
  unsupported,
  unsupportedFeature,
  unsupportedOp
};
