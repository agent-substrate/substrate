import { assert } from "./assert.js";
const defEnsure = (pred, expected) => (x, msg) => {
  x != null ? assert(
    () => pred(x),
    msg || `expected ${expected}, got ${typeof x}`
  ) : assert(false, `expected ${expected}, got ${x}`);
  return x;
};
const ensureArray = defEnsure((x) => Array.isArray(x), `array`);
const ensureBigInt = defEnsure(
  (x) => typeof x === "bigint",
  "bigint"
);
const ensureBoolean = defEnsure(
  (x) => typeof x === "boolean",
  "boolean"
);
const ensureFunction = defEnsure(
  (x) => typeof x === "function",
  "function"
);
const ensureIterable = defEnsure(
  (x) => typeof x[Symbol.iterator] === "function",
  "iterable"
);
const ensureNumber = defEnsure(
  (x) => typeof x === "number",
  "number"
);
const ensureString = defEnsure(
  (x) => typeof x === "string",
  "string"
);
const ensureSymbol = defEnsure(
  (x) => typeof x === "symbol",
  "symbol"
);
export {
  defEnsure,
  ensureArray,
  ensureBigInt,
  ensureBoolean,
  ensureFunction,
  ensureIterable,
  ensureNumber,
  ensureString,
  ensureSymbol
};
