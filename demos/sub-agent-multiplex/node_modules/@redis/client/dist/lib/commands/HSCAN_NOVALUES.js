"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.transformReply = exports.transformArguments = exports.IS_READ_ONLY = exports.FIRST_KEY_INDEX = void 0;
const HSCAN_1 = require("./HSCAN");
var HSCAN_2 = require("./HSCAN");
Object.defineProperty(exports, "FIRST_KEY_INDEX", { enumerable: true, get: function () { return HSCAN_2.FIRST_KEY_INDEX; } });
Object.defineProperty(exports, "IS_READ_ONLY", { enumerable: true, get: function () { return HSCAN_2.IS_READ_ONLY; } });
function transformArguments(key, cursor, options) {
    const args = (0, HSCAN_1.transformArguments)(key, cursor, options);
    args.push('NOVALUES');
    return args;
}
exports.transformArguments = transformArguments;
function transformReply([cursor, rawData]) {
    return {
        cursor: Number(cursor),
        keys: rawData
    };
}
exports.transformReply = transformReply;
