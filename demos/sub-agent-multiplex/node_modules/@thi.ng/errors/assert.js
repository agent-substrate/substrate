import { defError } from "./deferror.js";
const AssertionError = defError(() => "Assertion failed");
const assert = (typeof process !== "undefined" && process.env !== void 0 ? process.env.NODE_ENV !== "production" || !!process.env.UMBRELLA_ASSERTS : import.meta.env ? import.meta.env.MODE !== "production" || !!import.meta.env.UMBRELLA_ASSERTS || !!import.meta.env.VITE_UMBRELLA_ASSERTS : true) ? (test, msg) => {
  if (typeof test === "function" && !test() || !test) {
    throw new AssertionError(
      typeof msg === "function" ? msg() : msg
    );
  }
} : () => {
};
export {
  AssertionError,
  assert
};
