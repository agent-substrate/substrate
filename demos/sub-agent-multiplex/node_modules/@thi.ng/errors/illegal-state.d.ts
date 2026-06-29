export declare const IllegalStateError: {
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
export declare const illegalState: (msg?: any) => never;
//# sourceMappingURL=illegal-state.d.ts.map