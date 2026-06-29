export declare class BitInputStream {
    buffer: Uint8Array;
    protected start: number;
    protected limit: number;
    protected pos: number;
    protected bitPos: number;
    protected bit: number;
    constructor(buffer: Uint8Array, offset?: number, limit?: number);
    [Symbol.iterator](): Generator<number, void, unknown>;
    get length(): number;
    get position(): number;
    seek(pos: number): BitInputStream;
    read(wordSize?: number, safe?: boolean): number;
    readFields(fields: number[], safe?: boolean): number[];
    readWords(n: number, wordSize?: number, safe?: boolean): number[];
    readStruct(fields: [string, number][], safe?: boolean): any;
    readBit(safe?: boolean): number;
    protected _read(wordSize: number, safe?: boolean): number;
    protected checkLimit(requested: number): void;
}
//# sourceMappingURL=input.d.ts.map