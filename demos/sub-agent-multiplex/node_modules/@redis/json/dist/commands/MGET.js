"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.transformReply = exports.transformArguments = exports.IS_READ_ONLY = exports.FIRST_KEY_INDEX = void 0;
const _1 = require(".");
exports.FIRST_KEY_INDEX = 1;
exports.IS_READ_ONLY = true;
function transformArguments(keys, path) {
    return [
        'JSON.MGET',
        ...keys,
        path
    ];
}
exports.transformArguments = transformArguments;
function transformReply(reply) {
    return reply.map(_1.transformRedisJsonNullReply);
}
exports.transformReply = transformReply;
