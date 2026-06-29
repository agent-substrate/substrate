import { RedisCommandArgument } from '.';
import { HashExpiration } from './HEXPIRE';
export declare const FIRST_KEY_INDEX = 1;
export declare function transformArguments(key: RedisCommandArgument, fields: RedisCommandArgument | Array<RedisCommandArgument>, timestamp: number | Date, mode?: 'NX' | 'XX' | 'GT' | 'LT'): import(".").RedisCommandArguments;
export declare function transformReply(): Array<HashExpiration>;
