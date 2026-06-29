"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.transformArguments = exports.FIRST_KEY_INDEX = exports.HASH_EXPIRATION = void 0;
const generic_transformers_1 = require("./generic-transformers");
/**
 * @readonly
 * @enum {number}
 */
exports.HASH_EXPIRATION = {
    /** @property {number} */
    /** The field does not exist */
    FIELD_NOT_EXISTS: -2,
    /** @property {number} */
    /** Specified NX | XX | GT | LT condition not met */
    CONDITION_NOT_MET: 0,
    /** @property {number} */
    /** Expiration time was set or updated */
    UPDATED: 1,
    /** @property {number} */
    /** Field deleted because the specified expiration time is in the past */
    DELETED: 2
};
exports.FIRST_KEY_INDEX = 1;
function transformArguments(key, fields, seconds, mode) {
    const args = ['HEXPIRE', key, seconds.toString()];
    if (mode) {
        args.push(mode);
    }
    args.push('FIELDS');
    return (0, generic_transformers_1.pushVerdictArgument)(args, fields);
}
exports.transformArguments = transformArguments;
