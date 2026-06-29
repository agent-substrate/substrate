"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.encodeSignedDeviceIdentity = exports.configureSuccessfulPairing = exports.generateRegistrationNode = exports.generateLoginNode = exports.generateMobileNode = void 0;
const boom_1 = require("@hapi/boom");
const crypto_1 = require("crypto");
const WAProto_1 = require("../../WAProto");
const Defaults_1 = require("../Defaults");
const WABinary_1 = require("../WABinary");
const crypto_2 = require("./crypto");
const generics_1 = require("./generics");
const signal_1 = require("./signal");
const getUserAgent = (config) => {
    var _a, _b;
    const osVersion = config.mobile ? '15.3.1' : '0.1';
    const version = config.mobile ? [2, 22, 24] : config.version;
    const device = config.mobile ? 'iPhone_7' : 'Desktop';
    const manufacturer = config.mobile ? 'Apple' : '';
    const platform = config.mobile ? WAProto_1.proto.ClientPayload.UserAgent.Platform.IOS : WAProto_1.proto.ClientPayload.UserAgent.Platform.MACOS;
    const phoneId = config.mobile ? { phoneId: config.auth.creds.phoneId } : {};
    return {
        appVersion: {
            primary: version[0],
            secondary: version[1],
            tertiary: version[2],
        },
        platform,
        releaseChannel: WAProto_1.proto.ClientPayload.UserAgent.ReleaseChannel.RELEASE,
        mcc: ((_a = config.auth.creds.registration) === null || _a === void 0 ? void 0 : _a.phoneNumberMobileCountryCode) || '000',
        mnc: ((_b = config.auth.creds.registration) === null || _b === void 0 ? void 0 : _b.phoneNumberMobileNetworkCode) || '000',
        osVersion: osVersion,
        manufacturer,
        device,
        osBuildNumber: osVersion,
        localeLanguageIso6391: 'en',
        localeCountryIso31661Alpha2: 'US',
        ...phoneId
    };
};
const getClientPayload = (config) => {
    const payload = {
        connectType: WAProto_1.proto.ClientPayload.ConnectType.WIFI_UNKNOWN,
        connectReason: WAProto_1.proto.ClientPayload.ConnectReason.USER_ACTIVATED,
        userAgent: getUserAgent(config),
    };
    return payload;
};
const generateMobileNode = (config) => {
    if (!config.auth.creds) {
        throw new boom_1.Boom('No registration data found', { data: config });
    }
    const payload = {
        ...getClientPayload(config),
        sessionId: Math.floor(Math.random() * 999999999 + 1),
        shortConnect: true,
        connectAttemptCount: 0,
        device: 0,
        dnsSource: {
            appCached: false,
            dnsMethod: WAProto_1.proto.ClientPayload.DNSSource.DNSResolutionMethod.SYSTEM,
        },
        passive: false,
        pushName: 'test',
        username: Number(`${config.auth.creds.registration.phoneNumberCountryCode}${config.auth.creds.registration.phoneNumberNationalNumber}`),
    };
    return WAProto_1.proto.ClientPayload.fromObject(payload);
};
exports.generateMobileNode = generateMobileNode;
const generateLoginNode = (userJid, config) => {
    const { user, device } = (0, WABinary_1.jidDecode)(userJid);
    const payload = {
        ...getClientPayload(config),
        passive: true,
        username: +user,
        device: device,
    };
    return WAProto_1.proto.ClientPayload.fromObject(payload);
};
exports.generateLoginNode = generateLoginNode;
const getPlatformType = (platform) => {
    const platformType = platform.toUpperCase();
    return WAProto_1.proto.DeviceProps.PlatformType[platformType] || WAProto_1.proto.DeviceProps.PlatformType.DESKTOP;
};
const generateRegistrationNode = ({ registrationId, signedPreKey, signedIdentityKey }, config) => {
    // the app version needs to be md5 hashed
    // and passed in
    const appVersionBuf = (0, crypto_1.createHash)('md5')
        .update(config.version.join('.')) // join as string
        .digest();
    const companion = {
        os: config.browser[0],
        platformType: getPlatformType(config.browser[1]),
        requireFullSync: config.syncFullHistory,
    };
    const companionProto = WAProto_1.proto.DeviceProps.encode(companion).finish();
    const registerPayload = {
        ...getClientPayload(config),
        passive: false,
        devicePairingData: {
            buildHash: appVersionBuf,
            deviceProps: companionProto,
            eRegid: (0, generics_1.encodeBigEndian)(registrationId),
            eKeytype: Defaults_1.KEY_BUNDLE_TYPE,
            eIdent: signedIdentityKey.public,
            eSkeyId: (0, generics_1.encodeBigEndian)(signedPreKey.keyId, 3),
            eSkeyVal: signedPreKey.keyPair.public,
            eSkeySig: signedPreKey.signature,
        },
    };
    return WAProto_1.proto.ClientPayload.fromObject(registerPayload);
};
exports.generateRegistrationNode = generateRegistrationNode;
const configureSuccessfulPairing = (stanza, { advSecretKey, signedIdentityKey, signalIdentities }) => {
    const msgId = stanza.attrs.id;
    const pairSuccessNode = (0, WABinary_1.getBinaryNodeChild)(stanza, 'pair-success');
    const deviceIdentityNode = (0, WABinary_1.getBinaryNodeChild)(pairSuccessNode, 'device-identity');
    const platformNode = (0, WABinary_1.getBinaryNodeChild)(pairSuccessNode, 'platform');
    const deviceNode = (0, WABinary_1.getBinaryNodeChild)(pairSuccessNode, 'device');
    const businessNode = (0, WABinary_1.getBinaryNodeChild)(pairSuccessNode, 'biz');
    if (!deviceIdentityNode || !deviceNode) {
        throw new boom_1.Boom('Missing device-identity or device in pair success node', { data: stanza });
    }
    const bizName = businessNode === null || businessNode === void 0 ? void 0 : businessNode.attrs.name;
    const jid = deviceNode.attrs.jid;
    const { details, hmac } = WAProto_1.proto.ADVSignedDeviceIdentityHMAC.decode(deviceIdentityNode.content);
    // check HMAC matches
    const advSign = (0, crypto_2.hmacSign)(details, Buffer.from(advSecretKey, 'base64'));
    if (Buffer.compare(hmac, advSign) !== 0) {
        throw new boom_1.Boom('Invalid account signature');
    }
    const account = WAProto_1.proto.ADVSignedDeviceIdentity.decode(details);
    const { accountSignatureKey, accountSignature, details: deviceDetails } = account;
    // verify the device signature matches
    const accountMsg = Buffer.concat([Buffer.from([6, 0]), deviceDetails, signedIdentityKey.public]);
    if (!crypto_2.Curve.verify(accountSignatureKey, accountMsg, accountSignature)) {
        throw new boom_1.Boom('Failed to verify account signature');
    }
    // sign the details with our identity key
    const deviceMsg = Buffer.concat([Buffer.from([6, 1]), deviceDetails, signedIdentityKey.public, accountSignatureKey]);
    account.deviceSignature = crypto_2.Curve.sign(signedIdentityKey.private, deviceMsg);
    const identity = (0, signal_1.createSignalIdentity)(jid, accountSignatureKey);
    const accountEnc = (0, exports.encodeSignedDeviceIdentity)(account, false);
    const deviceIdentity = WAProto_1.proto.ADVDeviceIdentity.decode(account.details);
    const reply = {
        tag: 'iq',
        attrs: {
            to: WABinary_1.S_WHATSAPP_NET,
            type: 'result',
            id: msgId,
        },
        content: [
            {
                tag: 'pair-device-sign',
                attrs: {},
                content: [
                    {
                        tag: 'device-identity',
                        attrs: { 'key-index': deviceIdentity.keyIndex.toString() },
                        content: accountEnc
                    }
                ]
            }
        ]
    };
    const authUpdate = {
        account,
        me: { id: jid, name: bizName },
        signalIdentities: [
            ...(signalIdentities || []),
            identity
        ],
        platform: platformNode === null || platformNode === void 0 ? void 0 : platformNode.attrs.name
    };
    return {
        creds: authUpdate,
        reply
    };
};
exports.configureSuccessfulPairing = configureSuccessfulPairing;
const encodeSignedDeviceIdentity = (account, includeSignatureKey) => {
    var _a;
    account = { ...account };
    // set to null if we are not to include the signature key
    // or if we are including the signature key but it is empty
    if (!includeSignatureKey || !((_a = account.accountSignatureKey) === null || _a === void 0 ? void 0 : _a.length)) {
        account.accountSignatureKey = null;
    }
    return WAProto_1.proto.ADVSignedDeviceIdentity
        .encode(account)
        .finish();
};
exports.encodeSignedDeviceIdentity = encodeSignedDeviceIdentity;
