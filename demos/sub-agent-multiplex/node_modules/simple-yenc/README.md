# `simple-yenc`

`simple-yenc` is a minimalist yEnc and dynEncode encoder and decoder ECMAScript library for browser and NodeJS.

This library is designed to be simple to use and is optimized for size so that inclusion in a browser based application has very little impact.

## What is yEnc?

yEnc, or yEncode, is a very space efficient method of binary encoding. yEnc's overhead is around 1–2%,
compared to 33%–40% overhead for 6-bit encoding methods like Base64. yEnc has 252 of the 256 possible bytes available for use for encoding. NUL, LF, CR, and = are the only escaped characters.

### Read More
http://www.yenc.org/whatis.htm

## What is dynEncode?

dynEncode, or Dynamic Encode is an encoding scheme that dynamically selects the most efficient byte offset depending incoming data and the required escape characters. This encoding scheme is meant to be the most efficient encoding scheme given a set of escape characters and input data. Generally, it is a few percentage points more efficient than yEnc.

The only required escape character is `=`, which is used to denote an escaped value. Any other characters may or may not be escaped in the string depending on the destination's escape requirements.

Dynamic encoded strings have a configurable byte offset that is selected when the data is encoded. This offset shifts the encoded data around to avoid characters that must be escaped. For example, yEnc's offset is hardcoded to 42, whereas dynEncode can select an offset of 0-255 (all possible offsets).

Dynamic encoded strings are prepended with the below metadata that describes the parameters needed to decode the string into binary.

### **`dynEncode` Format**
Bytes | Name         | Range                         | Description
------|--------------|-------------------------------|--------------------------------------------------------------------------
00-08 | Signature    | `dynEncode`                   | Magic signature identifying the string as a dynEncode string.
09-10 | Version      | `0x00-0xFE` (`0xFF` reserved) | Base-16 string describing the version of dynEncode.

#### Version 0
Bytes | Name         | Range       | Description
------|--------------|-------------|--------------------------------------------------------------------------
00-08 | Signature    | `dynEncode` | Magic signature identifying the string as a dynEncode string.
09-10 | Version      | `0x00`      | Base-16 string describing the version of dynEncode.
11-12 | Byte Offset  | `0x00-0xFF` | Base-16 string describing the byte offset used when encoding.
13-xx | Encoded Data | ...         | Data contained in the dynEncode string.

#### Current Version (Version 1)
Bytes | Name            | Range                     | Description
------|--------------   | --------------------------|--------------------------------------------------------------------------
00-08 | Signature       | `dynEncode`               | Magic signature identifying the string as a dynEncode string.
09-10 | Version         | `0x01`                    | Base-16 string describing the version of dynEncode.
11-12 | Byte Offset     | `0x00-0xFF`               | Base-16 string describing the byte offset used when encoding.
13-20 | CRC 32 checksum | `0x00000000`-`0xFFFFFFFF` | Base-16 string encoding a 32bit signed integer (little endian) describing the checksum of the encoded data. <br> The checksum is validated during the decode process.
21-xx | Encoded Data    | ...                       | Data contained in the dynEncode string.

*dynEncode (Dynamic Encode), the above encoding scheme, and included implementation are copyright © 2022 Ethan Halsall and are licensed under the conditions of the [MIT License](https://github.com/eshaz/simple-yenc/blob/master/LICENSE).*

## Usage

Install via [NPM](https://www.npmjs.com/package/simple-yenc)

```javascript
// NodeJS
const yenc = require("simple-yenc");

// ES6 Import
import * as yenc from "simple-yenc";
```

### `encode()`

Encodes a Uint8Array, or array of integers 0-255, to a yEnc encoded string.

```javascript
// assuming `dataToEncode` is already defined

const encodedString = yenc.encode(dataToEncode);
```

### `dynamicEncode()`

Encodes a Uint8Array, or array of integers 0-255, to a dynEncoded string.

This function will iterate over the incoming data 255 times to select the byte offset with the least amount of overhead. Encoding may be relatively slow for large input data.

This function only encodes the current version

#### Parameters

```javascript
// assuming `dataToEncode` and `stringContainerCharacter` are already defined

const encodedString = yenc.encode(dataToEncode, stringContainerCharacter);
```

* First Parameter
  * `Array` of integers 0-255 of data to encode
* Second Parameter *optional* **default: `"`**
  * `string` representing the character used to declare the string the data will be stored. Escape characters are selected automatically base on JavaScript string escape requirements depending on this value.
  * Valid parameter values: ``` ` ``` (backtick), `"` (double quote), `'` (single quote) 
* Third Parameter *optional* **default: `yenc.crc32`**
  * CRC32 Function that is used to calculate the checksum.
  * The function must implement the `CRC32Function` interface

### `decode()`

Decodes a yEnc or dynEnc encoded string into a Uint8Array.

Note: If you are embedding the yEnc string in HTML, this function will automatically handle the [HTML character reference overrides](https://html.spec.whatwg.org/multipage/parsing.html#table-charref-overrides). The HTML `charset` must be set to a character encoding that allows for single byte character representations such as `cp1252`, `ISO-8859-1`, etc. See [Issue #1](https://github.com/eshaz/simple-yenc/issues/1) for more information.

This function decodes all version of dynEnc strings.

```javascript
// assuming `encodedString` is already defined

const decodedUint8Array = yenc.decode(encodedString);
```

* First Parameter
  * `string` yenc or dynEnc string
* Second Parameter *optional* **default: `yenc.crc32`**
  * CRC32 Function that was used to calculate the checksum for the dynamicEncoded string.
  * The function must implement the `CRC32Function` interface

### `stringify()`

Stringify adds escape characters for ```'\', '`', '${'``` to a yEnc encoded string so it can be stored within a Javascript string template.

You can access the encoded value as a string using [``` String.raw`` ```](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/raw).

This is useful for inlining binary in Javascript that would otherwise need to be base64 encoded.

When stored as a Javascript string, the string can be decoded by using the `decode` function.

```javascript
// assuming `dataToEncode` is already defined

const encodedString = yenc.encode(dataToEncode);
const stringified = yenc.strinfigy(encodedString);

fs.writeFileSync(
  "myfile.js",
  Buffer.concat(
    ["const encodedBinary = `", stringified, "`"].map(Buffer.from)
  ),
  { encoding: "binary" }
);
```

### `crc32()`

Default CRC32 function that calculates the checksum for dynamicEncoded strings.

Adapted from [GCC](https://gcc.gnu.org/git/?p=gcc.git;a=blob_plain;f=libiberty/crc32.c;hb=refs/heads/master).

This CRC can be specified as:
  * Width: `32`
  * Poly: `0x04c11db7`
  * Init: `0xffffffff`
  * RefIn: `false`
  * RefOut `false`
  * XorOut `0`

```typescript
// CRC32 Function Interface
(
  buf: Uint8Array,
  init: number = 0xffffffff,
  poly: number = 0x04c11db7
) => CRC32Function
```