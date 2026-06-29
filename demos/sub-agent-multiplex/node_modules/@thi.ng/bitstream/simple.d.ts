/**
 * Barebones alternative to {@link BitOutputStream} for word sizes <= 8 and with
 * minimal API surface. The returned object only exposes 2 functions:
 *
 * - `write(x, size)` - writes a single value of given bit size (default: 1 bit)
 * - `bytes()` - retrieve all bytes written so far
 *
 * @remarks
 * The internal backing buffer automatically resizes on demand. The optionally
 * provided `capacity` is only the initial buffer size.
 *
 * @param capacity - initial capacity
 */
export declare const bitWriter: (capacity?: number) => {
    write: (x: number, n?: number) => void;
    bytes: () => Uint8Array<ArrayBuffer>;
};
/**
 * Barebones alternative to {@link BitInputStream} for word sizes <= 8 and with
 * minimal API surface and WITHOUT bounds checking of any form! The returned
 * function reads `n` bits from the originally provided buffer.
 *
 * @param buf
 */
export declare const bitReader: (buf: Uint8Array | number[]) => (n?: number) => number;
/**
 * Wrapper for {@link bitReader} to read a 16bit word (big endian)
 *
 * @param read
 */
export declare const read16: (read: (n: number) => number) => number;
/**
 * Wrapper for {@link bitReader} to read a 24bit word (big endian)
 *
 * @param read
 */
export declare const read24: (read: (n: number) => number) => number;
/**
 * Wrapper for {@link bitReader} to read a 32bit word (big endian, unsigned)
 *
 * @param read
 */
export declare const read32: (read: (n: number) => number) => number;
//# sourceMappingURL=simple.d.ts.map