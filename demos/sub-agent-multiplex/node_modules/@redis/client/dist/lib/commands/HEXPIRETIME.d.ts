import { RedisCommandArgument } from '.';
export declare const HASH_EXPIRATION_TIME: {
    /** @property {number} */
    /** The field does not exist */
    readonly FIELD_NOT_EXISTS: -2;
    /** @property {number} */
    /** The field exists but has no associated expire */
    readonly NO_EXPIRATION: -1;
};
export declare const FIRST_KEY_INDEX = 1;
export declare const IS_READ_ONLY = true;
export declare function transformArguments(key: RedisCommandArgument, fields: RedisCommandArgument | Array<RedisCommandArgument>): import(".").RedisCommandArguments;
export declare function transformReply(): Array<number>;
