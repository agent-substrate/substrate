import { RedisCommandArgument, RedisCommandArguments } from '.';
import { ScanOptions } from './generic-transformers';
import { HScanRawReply } from './HSCAN';
export { FIRST_KEY_INDEX, IS_READ_ONLY } from './HSCAN';
export declare function transformArguments(key: RedisCommandArgument, cursor: number, options?: ScanOptions): RedisCommandArguments;
interface HScanNoValuesReply {
    cursor: number;
    keys: Array<RedisCommandArgument>;
}
export declare function transformReply([cursor, rawData]: HScanRawReply): HScanNoValuesReply;
