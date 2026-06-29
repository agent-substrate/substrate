/// <reference types="node" />
import { Socket } from 'net';
import { AbstractSocketClient } from './abstract-socket-client';
export declare class MobileSocketClient extends AbstractSocketClient {
    protected socket: Socket | null;
    get isOpen(): boolean;
    get isClosed(): boolean;
    get isClosing(): boolean;
    get isConnecting(): boolean;
    connect(): Promise<void>;
    close(): Promise<void>;
    send(str: string | Uint8Array, cb?: (err?: Error) => void): boolean;
}
