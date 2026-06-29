export declare const OutOfBoundsError: {
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
export declare const outOfBounds: (index: any) => never;
/**
 * Throws an {@link OutOfBoundsError} if `index` outside the `[min..max)` range.
 *
 * @param index -
 * @param min -
 * @param max -
 */
export declare const ensureIndex: (index: number, min: number, max: number) => false;
/**
 * Throws an {@link OutOfBoundsError} if either `x` or `y` is outside their
 * respective `[0..max)` range.
 *
 * @param x -
 * @param y -
 * @param maxX -
 * @param maxY -
 */
export declare const ensureIndex2: (x: number, y: number, maxX: number, maxY: number) => false;
//# sourceMappingURL=out-of-bounds.d.ts.map