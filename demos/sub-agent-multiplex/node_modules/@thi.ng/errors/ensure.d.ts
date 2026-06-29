/**
 * Higher-order function to define assurance assertions. Takes a `pred`icate
 * function and an `expected` (type) name, returns a new function which accepts
 * 2 args: an arbitrary value `x` and optional error `msg`. When called, checks
 * `x` for non-nullishness and if so applies given `pred`icate. If result is
 * false (or `x` is nullish) and iff {@link assert} is enabled, throws a
 * {@link AssertionError} with given `msg` (or a constructed default msg).
 * Otherwise function is a no-op.
 *
 * @param pred
 * @param expected
 */
export declare const defEnsure: <T>(pred: (x: any) => boolean, expected: string) => (x: any, msg?: string) => T;
/**
 * Only enabled if {@link assert} is enabled (otherwise no-op). Checks if `x` is
 * a JS array and if not throws {@link AssertionError}, optionally with given
 * `msg`.
 *
 * @remarks
 * See {@link defEnsure} for details.
 *
 * @param x
 * @param msg
 */
export declare const ensureArray: (x: any, msg?: string) => any[];
/**
 * Only enabled if {@link assert} is enabled (otherwise no-op). Checks if `x` is
 * a bigint and if not throws {@link AssertionError}, optionally with given
 * `msg`.
 *
 * @remarks
 * See {@link defEnsure} for details.
 *
 * @param x
 * @param msg
 */
export declare const ensureBigInt: (x: any, msg?: string) => bigint;
/**
 * Only enabled if {@link assert} is enabled (otherwise no-op). Checks if `x` is
 * a boolean and if not throws {@link AssertionError}, optionally with given
 * `msg`.
 *
 * @remarks
 * See {@link defEnsure} for details.
 *
 * @param x
 * @param msg
 */
export declare const ensureBoolean: (x: any, msg?: string) => boolean;
/**
 * Only enabled if {@link assert} is enabled (otherwise no-op). Checks if `x` is
 * a function and if not throws {@link AssertionError}, optionally with given
 * `msg`.
 *
 * @remarks
 * See {@link defEnsure} for details.
 *
 * @param x
 * @param msg
 */
export declare const ensureFunction: (x: any, msg?: string) => Function;
/**
 * Only enabled if {@link assert} is enabled (otherwise no-op). Checks if `x` is
 * an ES6 iterable and if not throws {@link AssertionError}, optionally with
 * given `msg`.
 *
 * @remarks
 * See {@link defEnsure} for details.
 *
 * @param x
 * @param msg
 */
export declare const ensureIterable: (x: any, msg?: string) => Iterable<any>;
/**
 * Only enabled if {@link assert} is enabled (otherwise no-op). Checks if `x` is
 * a number and if not throws {@link AssertionError}, optionally with given
 * `msg`.
 *
 * @remarks
 * See {@link defEnsure} for details.
 *
 * @param x
 * @param msg
 */
export declare const ensureNumber: (x: any, msg?: string) => number;
/**
 * Only enabled if {@link assert} is enabled (otherwise no-op). Checks if `x` is
 * a string and if not throws {@link AssertionError}, optionally with given
 * `msg`.
 *
 * @remarks
 * See {@link defEnsure} for details.
 *
 * @param x
 * @param msg
 */
export declare const ensureString: (x: any, msg?: string) => string;
/**
 * Only enabled if {@link assert} is enabled (otherwise no-op). Checks if `x` is
 * a symbol and if not throws {@link AssertionError}, optionally with given
 * `msg`.
 *
 * @remarks
 * See {@link defEnsure} for details.
 *
 * @param x
 * @param msg
 */
export declare const ensureSymbol: (x: any, msg?: string) => symbol;
//# sourceMappingURL=ensure.d.ts.map