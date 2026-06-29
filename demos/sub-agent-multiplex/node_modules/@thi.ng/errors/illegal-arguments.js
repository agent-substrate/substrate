import { defError } from "./deferror.js";
const IllegalArgumentError = defError(() => "illegal argument(s)");
const illegalArgs = (msg) => {
  throw new IllegalArgumentError(msg);
};
export {
  IllegalArgumentError,
  illegalArgs
};
