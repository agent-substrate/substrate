export declare const AssertionError: {
    new (msg?: any): {
        origMessage: string;
        name: string;
        message: string;
        stack?: string;
        cause?: unknown;
    };
    captureStackTrace(targetObject: object, constructorOpt?: Function): void;
    prepareStackTrace(err: Error, stackTraces: NodeJS.CallSite[]): any;
    stackTraceLimit: number;
};
/**
 * Takes a `test` result or predicate function without args and throws error
 * with given `msg` if test failed (i.e. is falsy).
 *
 * @remarks
 * The function is only enabled if `process.env.NODE_ENV != "production"` or if
 * the `UMBRELLA_ASSERTS` or `VITE_UMBRELLA_ASSERTS` env var is set to 1.
 */
export declare const assert: (test: boolean | (() => boolean), msg?: string | (() => string)) => void;
//# sourceMappingURL=assert.d.ts.map