"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.mobileRegisterFetch = exports.mobileRegisterEncrypt = exports.mobileRegister = exports.mobileRegisterExists = exports.mobileRegisterCode = exports.registrationParams = exports.makeRegistrationSocket = void 0;
/* eslint-disable camelcase */
const axios_1 = __importDefault(require("axios"));
const Defaults_1 = require("../Defaults");
const crypto_1 = require("../Utils/crypto");
const WABinary_1 = require("../WABinary");
const business_1 = require("./business");
function urlencode(str) {
    return str.replace(/-/g, '%2d').replace(/_/g, '%5f').replace(/~/g, '%7e');
}
const validRegistrationOptions = (config) => (config === null || config === void 0 ? void 0 : config.phoneNumberCountryCode) &&
    config.phoneNumberNationalNumber &&
    config.phoneNumberMobileCountryCode;
const makeRegistrationSocket = (config) => {
    const sock = (0, business_1.makeBusinessSocket)(config);
    const register = async (code) => {
        if (!validRegistrationOptions(config.auth.creds.registration)) {
            throw new Error('please specify the registration options');
        }
        const result = await mobileRegister({ ...sock.authState.creds, ...sock.authState.creds.registration, code }, config.options);
        sock.authState.creds.me = {
            id: (0, WABinary_1.jidEncode)(result.login, 's.whatsapp.net'),
            name: '~'
        };
        sock.authState.creds.registered = true;
        sock.ev.emit('creds.update', sock.authState.creds);
        return result;
    };
    const requestRegistrationCode = async (registrationOptions) => {
        registrationOptions = registrationOptions || config.auth.creds.registration;
        if (!validRegistrationOptions(registrationOptions)) {
            throw new Error('Invalid registration options');
        }
        sock.authState.creds.registration = registrationOptions;
        sock.ev.emit('creds.update', sock.authState.creds);
        return mobileRegisterCode({ ...config.auth.creds, ...registrationOptions }, config.options);
    };
    return {
        ...sock,
        register,
        requestRegistrationCode,
    };
};
exports.makeRegistrationSocket = makeRegistrationSocket;
function convertBufferToUrlHex(buffer) {
    var id = '';
    buffer.forEach((x) => {
        // encode random identity_id buffer as percentage url encoding
        id += `%${x.toString(16).padStart(2, '0').toLowerCase()}`;
    });
    return id;
}
function registrationParams(params) {
    const e_regid = Buffer.alloc(4);
    e_regid.writeInt32BE(params.registrationId);
    const e_skey_id = Buffer.alloc(3);
    e_skey_id.writeInt16BE(params.signedPreKey.keyId);
    params.phoneNumberCountryCode = params.phoneNumberCountryCode.replace('+', '').trim();
    params.phoneNumberNationalNumber = params.phoneNumberNationalNumber.replace(/[/-\s)(]/g, '').trim();
    return {
        cc: params.phoneNumberCountryCode,
        in: params.phoneNumberNationalNumber,
        Rc: '0',
        lg: 'en',
        lc: 'GB',
        mistyped: '6',
        authkey: Buffer.from(params.noiseKey.public).toString('base64url'),
        e_regid: e_regid.toString('base64url'),
        e_keytype: 'BQ',
        e_ident: Buffer.from(params.signedIdentityKey.public).toString('base64url'),
        // e_skey_id: e_skey_id.toString('base64url'),
        e_skey_id: 'AAAA',
        e_skey_val: Buffer.from(params.signedPreKey.keyPair.public).toString('base64url'),
        e_skey_sig: Buffer.from(params.signedPreKey.signature).toString('base64url'),
        fdid: params.phoneId,
        network_ratio_type: '1',
        expid: params.deviceId,
        simnum: '1',
        hasinrc: '1',
        pid: Math.floor(Math.random() * 1000).toString(),
        id: convertBufferToUrlHex(params.identityId),
        backup_token: convertBufferToUrlHex(params.backupToken),
        token: (0, crypto_1.md5)(Buffer.concat([Defaults_1.MOBILE_TOKEN, Buffer.from(params.phoneNumberNationalNumber)])).toString('hex'),
        fraud_checkpoint_code: params.captcha,
    };
}
exports.registrationParams = registrationParams;
/**
 * Requests a registration code for the given phone number.
 */
function mobileRegisterCode(params, fetchOptions) {
    return mobileRegisterFetch('/code', {
        params: {
            ...registrationParams(params),
            mcc: `${params.phoneNumberMobileCountryCode}`.padStart(3, '0'),
            mnc: `${params.phoneNumberMobileNetworkCode || '001'}`.padStart(3, '0'),
            sim_mcc: '000',
            sim_mnc: '000',
            method: (params === null || params === void 0 ? void 0 : params.method) || 'sms',
            reason: '',
            hasav: '1'
        },
        ...fetchOptions,
    });
}
exports.mobileRegisterCode = mobileRegisterCode;
function mobileRegisterExists(params, fetchOptions) {
    return mobileRegisterFetch('/exist', {
        params: registrationParams(params),
        ...fetchOptions
    });
}
exports.mobileRegisterExists = mobileRegisterExists;
/**
 * Registers the phone number on whatsapp with the received OTP code.
 */
async function mobileRegister(params, fetchOptions) {
    //const result = await mobileRegisterFetch(`/reg_onboard_abprop?cc=${params.phoneNumberCountryCode}&in=${params.phoneNumberNationalNumber}&rc=0`)
    return mobileRegisterFetch('/register', {
        params: { ...registrationParams(params), code: params.code.replace('-', '') },
        ...fetchOptions,
    });
}
exports.mobileRegister = mobileRegister;
/**
 * Encrypts the given string as AEAD aes-256-gcm with the public whatsapp key and a random keypair.
 */
function mobileRegisterEncrypt(data) {
    const keypair = crypto_1.Curve.generateKeyPair();
    const key = crypto_1.Curve.sharedKey(keypair.private, Defaults_1.REGISTRATION_PUBLIC_KEY);
    const buffer = (0, crypto_1.aesEncryptGCM)(Buffer.from(data), new Uint8Array(key), Buffer.alloc(12), Buffer.alloc(0));
    return Buffer.concat([Buffer.from(keypair.public), buffer]).toString('base64url');
}
exports.mobileRegisterEncrypt = mobileRegisterEncrypt;
async function mobileRegisterFetch(path, opts = {}) {
    let url = `${Defaults_1.MOBILE_REGISTRATION_ENDPOINT}${path}`;
    if (opts.params) {
        const parameter = [];
        for (const param in opts.params) {
            if (opts.params[param] !== null && opts.params[param] !== undefined) {
                parameter.push(param + '=' + urlencode(opts.params[param]));
            }
        }
        url += `?${parameter.join('&')}`;
        delete opts.params;
    }
    if (!opts.headers) {
        opts.headers = {};
    }
    opts.headers['User-Agent'] = Defaults_1.MOBILE_USERAGENT;
    const response = await (0, axios_1.default)(url, opts);
    var json = response.data;
    if (response.status > 300 || json.reason) {
        throw json;
    }
    if (json.status && !['ok', 'sent'].includes(json.status)) {
        throw json;
    }
    return json;
}
exports.mobileRegisterFetch = mobileRegisterFetch;
