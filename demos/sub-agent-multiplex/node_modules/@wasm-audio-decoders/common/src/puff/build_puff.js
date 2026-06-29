import fs from "fs";
import { dynamicEncode } from "simple-yenc";

const puffWasmPath = "src/common/src/puff/Puff.wasm";
const wasmCommonPath = "src/common/src/WASMAudioDecoderCommon.js";

const puffWasm = fs.readFileSync(puffWasmPath);

const puffEncoded = dynamicEncode(puffWasm, "`");

const wasmCommon = fs.readFileSync(wasmCommonPath).toString();

const puffString = wasmCommon.match(/const puffString = String.raw`.*`;/s)[0];

const wasmStartIdx = wasmCommon.indexOf(puffString);
const wasmEndIdx = wasmStartIdx + puffString.length;

// Concatenate the strings as buffers to preserve extended ascii
const wasmCommonWithPuff = Buffer.concat(
  [
    wasmCommon.substring(0, wasmStartIdx),
    "const puffString = String.raw`",
    puffEncoded,
    "`;",
    wasmCommon.substring(wasmEndIdx),
  ].map((string) => Buffer.from(string, { encoding: "binary" })),
);

fs.writeFileSync(wasmCommonPath, wasmCommonWithPuff, { encoding: "binary" });

console.log(puffWasm.length);
