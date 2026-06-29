'use strict';

/**
 * @file
 *
 * Copyright 2018 FutoIn Project (https://futoin.org)
 * Copyright 2018 Andrey Galkin <andrey@futoin.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const { Buffer } = require( 'buffer' );
const hkdf = require( './hkdf' );

/**
 * Encode HKDF context parameter in TLS v1.3 style based on RFC8446 TLS v1.3.
 *
 * @func
 * @alias tls.info
 * @param {integer} length - length of output keying material in octets
 * @param {string} label - ASCII label
 * @param {Buffer|string} context - Binary context or empty string
 * @returns {Buffer} A buffer with encoded HKDF context
 *
 * @note label and context are limited to 255 bytes!
 */
const tls_info = ( length, label, context ) => {
    const b_context = Buffer.isBuffer( context ) ? context : Buffer.from( context, 'hex' );

    const label_length = label.length;
    const context_length = b_context.length;

    const b_info = Buffer.allocUnsafe( 4 + label_length + context_length );
    b_info.writeUInt16BE( length, 0 );
    b_info.writeUInt8( label_length, 2 );
    b_info.write( label, 3, label_length, 'ascii' );
    b_info.writeUInt8( context_length, 3 + label_length );
    b_context.copy( b_info, 4 + label_length, 0, context_length );

    return b_info;
};

/**
 * TLS-HKDF expand label action - a HKDF-Expand-Label variation based on RFC8446 TLS v1.3.
 *
 * @func
 * @alias tls.expand_label
 * @param {string} hash - Hash algorithm (as in underlying Node.js crypto library)
 * @param {integer} hash_len - Hash digest length
 * @param {Buffer|string} prk - A buffer with pseudorandom key
 * @param {integer} length - length of output keying material in octets
 * @param {string} label - ASCII label
 * @param {Buffer|string} context - Binary context or empty string
 * @returns {Buffer} A buffer with output keying material
 *
 * @note label and context are limited to 255 bytes!
 */
const tls_expand_label = ( hash, hash_len, prk, length, label, context ) => {
    return hkdf.expand( hash, hash_len, prk, length, tls_info( length, label, context ) );
};

/**
 * TLS v1.3 HKDF-extract + HKFD-Expand-Label action
 *
 * @param {Buffer|string} ikm - Initial Keying Material
 * @param {integer} length - Required byte length of output
 * @param {Buffer|string} salt='' - Optional salt (required by fact)
 * @param {Buffer|string} label='' - Optional label (required by fact)
 * @param {Buffer|string} info='' - Optional context (safe to skip)
 * @param {string} hash='SHA-256' - HMAC hash function to use
 * @returns {Buffer} Raw buffer with derived key of @p length bytes
 *
 * @note label and context are limited to 255 bytes!
 */
function tls( ikm, length, { salt='', label='', context='', hash='SHA-256' } = {} ) {
    hash = hash.toLowerCase().replace( '-', '' );

    // 0. Hash length
    const hash_len = hkdf.hash_length( hash );

    // 1. extract
    const prk = hkdf.extract( hash, hash_len, ikm, salt );

    // 2. expand
    return tls_expand_label( hash, hash_len, prk, length, label, context );
}

Object.defineProperties( tls, {
    info : {
        configurable: false,
        enumerable: false,
        writable: false,
        value: tls_info,
    },
    expand_label : {
        configurable: false,
        enumerable: false,
        writable: false,
        value: tls_expand_label,
    },
} );

module.exports = tls;
