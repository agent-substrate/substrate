export declare const IllegalArityError: {
    new (msg?: number | undefined): {
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
export declare const illegalArity: (n: number) => never;
//# sourceMappingURL=illegal-arity.d.ts.map