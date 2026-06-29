"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.transformArguments = exports.IS_READ_ONLY = exports.FIRST_KEY_INDEX = void 0;
const generic_transformers_1 = require("./generic-transformers");
exports.FIRST_KEY_INDEX = 1;
exports.IS_READ_ONLY = true;
function transformArguments(key, fields, timestamp, mode) {
    const args = ['HPEXPIREAT', key, (0, generic_transformers_1.transformPXAT)(timestamp)];
    if (mode) {
        args.push(mode);
    }
    args.push('FIELDS');
    return (0, generic_transformers_1.pushVerdictArgument)(args, fields);
}
exports.transformArguments = transformArguments;
