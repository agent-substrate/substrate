import { Labels, TimeSeriesDuplicatePolicies } from '.';
import { TsIgnoreOptions } from './ADD';
export declare const FIRST_KEY_INDEX = 1;
interface AlterOptions {
    RETENTION?: number;
    CHUNK_SIZE?: number;
    DUPLICATE_POLICY?: TimeSeriesDuplicatePolicies;
    LABELS?: Labels;
    IGNORE?: TsIgnoreOptions;
}
export declare function transformArguments(key: string, options?: AlterOptions): Array<string>;
export declare function transformReply(): 'OK';
export {};
