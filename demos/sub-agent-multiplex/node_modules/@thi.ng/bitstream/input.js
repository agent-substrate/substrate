import { illegalArgs } from "@thi.ng/errors/illegal-arguments";
import { illegalState } from "@thi.ng/errors/illegal-state";
const U32 = 4294967296;
class BitInputStream {
  buffer;
  start;
  limit;
  pos;
  bitPos;
  bit;
  constructor(buffer, offset = 0, limit = buffer.length << 3) {
    this.buffer = buffer;
    this.start = offset;
    this.limit = limit;
    this.seek(offset);
  }
  *[Symbol.iterator]() {
    let j = this.start;
    let i = j >>> 3;
    let b = 7 - (j & 7);
    while (j < this.limit) {
      yield this.buffer[i] >>> b & 1;
      if (--b < 0) {
        i++;
        b = 7;
      }
      j++;
    }
  }
  get length() {
    return this.limit;
  }
  get position() {
    return this.bitPos;
  }
  seek(pos) {
    if (pos < this.start || pos >= this.limit) {
      illegalArgs(`seek pos out of bounds: ${pos}`);
    }
    this.pos = pos >>> 3;
    this.bit = 8 - (pos & 7);
    this.bitPos = pos;
    return this;
  }
  read(wordSize = 1, safe = true) {
    if (wordSize > 32) {
      return this.read(wordSize - 32, safe) * U32 + this.read(32, safe);
    } else if (wordSize > 8) {
      let out = 0;
      let n = wordSize & -8;
      let msb = wordSize - n;
      if (msb > 0) {
        out = this._read(msb, safe);
      }
      while (n > 0) {
        out = (out << 8 | this._read(8, safe)) >>> 0;
        n -= 8;
      }
      return out;
    } else {
      return this._read(wordSize, safe);
    }
  }
  readFields(fields, safe = true) {
    return fields.map((word) => this.read(word, safe));
  }
  readWords(n, wordSize = 8, safe = true) {
    let out = [];
    while (n-- > 0) {
      out.push(this.read(wordSize, safe));
    }
    return out;
  }
  readStruct(fields, safe = true) {
    return fields.reduce((acc, [id, word]) => {
      return acc[id] = this.read(word, safe), acc;
    }, {});
  }
  readBit(safe = true) {
    safe && this.checkLimit(1);
    this.bit--;
    this.bitPos++;
    let out = this.buffer[this.pos] >>> this.bit & 1;
    if (this.bit === 0) {
      this.pos++;
      this.bit = 8;
    }
    return out;
  }
  _read(wordSize, safe = true) {
    safe && this.checkLimit(wordSize);
    let l = this.bit - wordSize, out;
    if (l >= 0) {
      this.bit = l;
      out = this.buffer[this.pos] >>> l & (1 << wordSize) - 1;
      if (l === 0) {
        this.pos++;
        this.bit = 8;
      }
    } else {
      out = (this.buffer[this.pos++] & (1 << this.bit) - 1) << -l;
      this.bit = 8 + l;
      out = out | this.buffer[this.pos] >>> this.bit;
    }
    this.bitPos += wordSize;
    return out;
  }
  checkLimit(requested) {
    if (this.bitPos + requested > this.limit) {
      illegalState(`can't read past EOF`);
    }
  }
}
export {
  BitInputStream
};
