import { BitInputStream } from "./input.js";
export declare class BitOutputStream {
    buffer: Uint8Array;
    protected start: number;
    protected pos: number;
    protected bit: number;
    protected bitPos: number;
    constructor(buffer?: number | Uint8Array, offset?: number);
    get position(): number;
    seek(pos: number): BitOutputStream;
    bytes(): Uint8Array<ArrayBuffer>;
    reader(from?: number): BitInputStream;
    write(x: number, wordSize?: number): this;
    writeWords(input: Iterable<number>, wordSize?: number): void;
    writeBit(x: number): this;
    protected _write(x: number, wordSize: number): this;
    protected ensureSize(): void;
}
//# sourceMappingURL=output.d.ts.map