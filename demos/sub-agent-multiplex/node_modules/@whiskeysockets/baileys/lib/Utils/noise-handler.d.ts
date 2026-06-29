/// <reference types="node" />
import { Logger } from 'pino';
import { proto } from '../../WAProto';
import { KeyPair } from '../Types';
import { BinaryNode } from '../WABinary';
export declare const makeNoiseHandler: ({ keyPair: { private: privateKey, public: publicKey }, NOISE_HEADER, mobile, logger, }: {
    keyPair: KeyPair;
    NOISE_HEADER: Uint8Array;
    mobile: boolean;
    logger: Logger;
}) => {
    encrypt: (plaintext: Uint8Array) => Buffer;
    decrypt: (ciphertext: Uint8Array) => Buffer;
    authenticate: (data: Uint8Array) => void;
    mixIntoKey: (data: Uint8Array) => void;
    finishInit: () => void;
    processHandshake: ({ serverHello }: proto.HandshakeMessage, noiseKey: KeyPair) => Buffer;
    encodeFrame: (data: Buffer | Uint8Array) => Buffer;
    decodeFrame: (newData: Buffer | Uint8Array, onFrame: (buff: Uint8Array | BinaryNode) => void) => void;
};
