import { defError } from "./deferror.js";
const IllegalStateError = defError(() => "illegal state");
const illegalState = (msg) => {
  throw new IllegalStateError(msg);
};
export {
  IllegalStateError,
  illegalState
};
