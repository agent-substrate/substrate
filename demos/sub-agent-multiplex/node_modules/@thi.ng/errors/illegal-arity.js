import { defError } from "./deferror.js";
const IllegalArityError = defError(() => "illegal arity");
const illegalArity = (n) => {
  throw new IllegalArityError(n);
};
export {
  IllegalArityError,
  illegalArity
};
