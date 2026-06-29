import { defError } from "./deferror.js";
const OutOfBoundsError = defError(() => "index out of bounds");
const outOfBounds = (index) => {
  throw new OutOfBoundsError(index);
};
const ensureIndex = (index, min, max) => (index < min || index >= max) && outOfBounds(index);
const ensureIndex2 = (x, y, maxX, maxY) => (x < 0 || x >= maxX || y < 0 || y >= maxY) && outOfBounds([x, y]);
export {
  OutOfBoundsError,
  ensureIndex,
  ensureIndex2,
  outOfBounds
};
