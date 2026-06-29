const bitWriter = (capacity = 16) => {
  let buf = new Uint8Array(capacity);
  let pos = 0;
  let bit = 8;
  const ensure = () => {
    if (++pos === buf.length) {
      let b = new Uint8Array(buf.length << 1);
      b.set(buf);
      buf = b;
    }
  };
  return {
    write: (x, n = 1) => {
      x &= (1 << n) - 1;
      let b = bit - n;
      let m = bit < 8 ? ~((1 << bit) - 1) : 0;
      if (b >= 0) {
        m |= (1 << b) - 1;
        buf[pos] = buf[pos] & m | x << b & ~m;
        if (b === 0) {
          ensure();
          bit = 8;
        } else {
          bit = b;
        }
      } else {
        bit = 8 + b;
        buf[pos] = buf[pos] & m | x >>> -b & ~m;
        ensure();
        buf[pos] = buf[pos] & (1 << bit) - 1 | x << bit & 255;
      }
    },
    bytes: () => buf.slice(0, pos + (bit & 7 ? 1 : 0))
  };
};
const bitReader = (buf) => {
  let p = 0;
  let b = 8;
  return (n = 1) => {
    let l = b - n;
    let out;
    if (l >= 0) {
      b = l;
      out = buf[p] >>> l & (1 << n) - 1;
      if (!l) {
        p++;
        b = 8;
      }
    } else {
      out = (buf[p++] & (1 << b) - 1) << -l;
      b = 8 + l;
      out = out | buf[p] >>> b;
    }
    return out;
  };
};
const read16 = (read) => read(8) << 8 | read(8);
const read24 = (read) => read16(read) << 8 | read(8);
const read32 = (read) => (read16(read) << 16 | read16(read)) >>> 0;
export {
  bitReader,
  bitWriter,
  read16,
  read24,
  read32
};
