"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.transformArguments = exports.IS_READ_ONLY = exports.FIRST_KEY_INDEX = exports.HASH_EXPIRATION_TIME = void 0;
const generic_transformers_1 = require("./generic-transformers");
exports.HASH_EXPIRATION_TIME = {
    /** @property {number} */
    /** The field does not exist */
    FIELD_NOT_EXISTS: -2,
    /** @property {number} */
    /** The field exists but has no associated expire */
    NO_EXPIRATION: -1,
};
exports.FIRST_KEY_INDEX = 1;
exports.IS_READ_ONLY = true;
function transformArguments(key, fields) {
    return (0, generic_transformers_1.pushVerdictArgument)(['HEXPIRETIME', key, 'FIELDS'], fields);
}
exports.transformArguments = transformArguments;
