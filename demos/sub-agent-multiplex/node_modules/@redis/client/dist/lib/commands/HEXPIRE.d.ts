import { RedisCommandArgument } from '.';
/**
 * @readonly
 * @enum {number}
 */
export declare const HASH_EXPIRATION: {
    /** @property {number} */
    /** The field does not exist */
    readonly FIELD_NOT_EXISTS: -2;
    /** @property {number} */
    /** Specified NX | XX | GT | LT condition not met */
    readonly CONDITION_NOT_MET: 0;
    /** @property {number} */
    /** Expiration time was set or updated */
    readonly UPDATED: 1;
    /** @property {number} */
    /** Field deleted because the specified expiration time is in the past */
    readonly DELETED: 2;
};
export type HashExpiration = typeof HASH_EXPIRATION[keyof typeof HASH_EXPIRATION];
export declare const FIRST_KEY_INDEX = 1;
export declare function transformArguments(key: RedisCommandArgument, fields: RedisCommandArgument | Array<RedisCommandArgument>, seconds: number, mode?: 'NX' | 'XX' | 'GT' | 'LT'): import(".").RedisCommandArguments;
export declare function transformReply(): Array<HashExpiration>;
