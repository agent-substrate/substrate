/// <reference types="node" />
import type { BinaryNode, BinaryNodeCodingOptions } from './types';
export declare const encodeBinaryNode: ({ tag, attrs, content }: BinaryNode, opts?: Pick<BinaryNodeCodingOptions, 'TAGS' | 'TOKEN_MAP'>, buffer?: number[]) => Buffer;
