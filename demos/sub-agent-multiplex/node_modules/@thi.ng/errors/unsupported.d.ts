export declare const UnsupportedOperationError: {
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
 * Throws {@link UnsupportedOperationError} error.
 *
 * @param msg
 */
export declare const unsupportedOp: (msg?: any) => never;
/**
 * @deprecated use {@link unsupportedOp}
 */
export declare const unsupported: (msg?: any) => never;
export declare const UnsupportedFeatureError: {
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
 * Throws {@link UnsupportedFeatureError} error.
 *
 * @param msg
 */
export declare const unsupportedFeature: (msg?: any) => never;
//# sourceMappingURL=unsupported.d.ts.map