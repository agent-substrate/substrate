export interface CustomError extends Error {
    /**
     * The original message given to the error ctor (prior to applying
     * prefix/suffix). If none was given this will be an empty string.
     */
    origMessage: string;
}
/**
 * Defines a custom error type/class which implements the {@link CustomError}
 * interface.
 *
 * @remarks
 * All error types in this package are defined via this function.
 *
 * @param prefix
 * @param suffix
 */
export declare const defError: <T = string>(prefix: (msg?: T) => string, suffix?: (msg?: T) => string) => {
    new (msg?: T): {
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
//# sourceMappingURL=deferror.d.ts.map