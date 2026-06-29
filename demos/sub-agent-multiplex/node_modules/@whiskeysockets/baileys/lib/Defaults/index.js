"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEFAULT_CACHE_TTLS = exports.INITIAL_PREKEY_COUNT = exports.MIN_PREKEY_COUNT = exports.MEDIA_KEYS = exports.MEDIA_HKDF_KEY_MAPPING = exports.MEDIA_PATH_MAP = exports.DEFAULT_CONNECTION_CONFIG = exports.PROCESSABLE_HISTORY_TYPES = exports.WA_CERT_DETAILS = exports.URL_EXCLUDE_REGEX = exports.URL_REGEX = exports.MOBILE_NOISE_HEADER = exports.PROTOCOL_VERSION = exports.NOISE_WA_HEADER = exports.KEY_BUNDLE_TYPE = exports.DICT_VERSION = exports.NOISE_MODE = exports.REGISTRATION_PUBLIC_KEY = exports.MOBILE_USERAGENT = exports.MOBILE_REGISTRATION_ENDPOINT = exports.MOBILE_TOKEN = exports.WA_DEFAULT_EPHEMERAL = exports.PHONE_CONNECTION_CB = exports.DEF_TAG_PREFIX = exports.DEF_CALLBACK_PREFIX = exports.MOBILE_PORT = exports.MOBILE_ENDPOINT = exports.DEFAULT_ORIGIN = exports.PHONENUMBER_MCC = exports.UNAUTHORIZED_CODES = void 0;
const crypto_1 = require("crypto");
const WAProto_1 = require("../../WAProto");
const libsignal_1 = require("../Signal/libsignal");
const Utils_1 = require("../Utils");
const logger_1 = __importDefault(require("../Utils/logger"));
const baileys_version_json_1 = require("./baileys-version.json");
const phonenumber_mcc_json_1 = __importDefault(require("./phonenumber-mcc.json"));
exports.UNAUTHORIZED_CODES = [401, 403, 419];
exports.PHONENUMBER_MCC = phonenumber_mcc_json_1.default;
exports.DEFAULT_ORIGIN = 'https://web.whatsapp.com';
exports.MOBILE_ENDPOINT = 'g.whatsapp.net';
exports.MOBILE_PORT = 443;
exports.DEF_CALLBACK_PREFIX = 'CB:';
exports.DEF_TAG_PREFIX = 'TAG:';
exports.PHONE_CONNECTION_CB = 'CB:Pong';
exports.WA_DEFAULT_EPHEMERAL = 7 * 24 * 60 * 60;
const WA_VERSION = '2.23.14.82';
const WA_VERSION_HASH = (0, crypto_1.createHash)('md5').update(WA_VERSION).digest('hex');
exports.MOBILE_TOKEN = Buffer.from('0a1mLfGUIBVrMKF1RdvLI5lkRBvof6vn0fD2QRSM' + WA_VERSION_HASH);
exports.MOBILE_REGISTRATION_ENDPOINT = 'https://v.whatsapp.net/v2';
exports.MOBILE_USERAGENT = `WhatsApp/${WA_VERSION} iOS/15.3.1 Device/Apple-iPhone_7`;
exports.REGISTRATION_PUBLIC_KEY = Buffer.from([
    5, 142, 140, 15, 116, 195, 235, 197, 215, 166, 134, 92, 108, 60, 132, 56, 86, 176, 97, 33, 204, 232, 234, 119, 77,
    34, 251, 111, 18, 37, 18, 48, 45,
]);
exports.NOISE_MODE = 'Noise_XX_25519_AESGCM_SHA256\0\0\0\0';
exports.DICT_VERSION = 2;
exports.KEY_BUNDLE_TYPE = Buffer.from([5]);
exports.NOISE_WA_HEADER = Buffer.from([87, 65, 6, exports.DICT_VERSION]); // last is "DICT_VERSION"
exports.PROTOCOL_VERSION = [5, 2];
exports.MOBILE_NOISE_HEADER = Buffer.concat([Buffer.from('WA'), Buffer.from(exports.PROTOCOL_VERSION)]);
/** from: https://stackoverflow.com/questions/3809401/what-is-a-good-regular-expression-to-match-a-url */
exports.URL_REGEX = /(http(s)?:\/\/.)?(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)/;
exports.URL_EXCLUDE_REGEX = /.*@.*/;
exports.WA_CERT_DETAILS = {
    SERIAL: 0,
};
exports.PROCESSABLE_HISTORY_TYPES = [
    WAProto_1.proto.Message.HistorySyncNotification.HistorySyncType.INITIAL_BOOTSTRAP,
    WAProto_1.proto.Message.HistorySyncNotification.HistorySyncType.PUSH_NAME,
    WAProto_1.proto.Message.HistorySyncNotification.HistorySyncType.RECENT,
    WAProto_1.proto.Message.HistorySyncNotification.HistorySyncType.FULL
];
exports.DEFAULT_CONNECTION_CONFIG = {
    version: baileys_version_json_1.version,
    browser: Utils_1.Browsers.baileys('Chrome'),
    waWebSocketUrl: 'wss://web.whatsapp.com/ws/chat',
    connectTimeoutMs: 20000,
    keepAliveIntervalMs: 30000,
    logger: logger_1.default.child({ class: 'baileys' }),
    printQRInTerminal: false,
    emitOwnEvents: true,
    defaultQueryTimeoutMs: 60000,
    customUploadHosts: [],
    retryRequestDelayMs: 250,
    maxMsgRetryCount: 5,
    fireInitQueries: true,
    auth: undefined,
    markOnlineOnConnect: true,
    syncFullHistory: false,
    patchMessageBeforeSending: msg => msg,
    shouldSyncHistoryMessage: () => true,
    shouldIgnoreJid: () => false,
    linkPreviewImageThumbnailWidth: 192,
    transactionOpts: { maxCommitRetries: 10, delayBetweenTriesMs: 3000 },
    generateHighQualityLinkPreview: false,
    options: {},
    appStateMacVerification: {
        patch: false,
        snapshot: false,
    },
    getMessage: async () => undefined,
    makeSignalRepository: libsignal_1.makeLibSignalRepository
};
exports.MEDIA_PATH_MAP = {
    image: '/mms/image',
    video: '/mms/video',
    document: '/mms/document',
    audio: '/mms/audio',
    sticker: '/mms/image',
    'thumbnail-link': '/mms/image',
    'product-catalog-image': '/product/image',
    'md-app-state': '',
    'md-msg-hist': '/mms/md-app-state',
};
exports.MEDIA_HKDF_KEY_MAPPING = {
    'audio': 'Audio',
    'document': 'Document',
    'gif': 'Video',
    'image': 'Image',
    'ppic': '',
    'product': 'Image',
    'ptt': 'Audio',
    'sticker': 'Image',
    'video': 'Video',
    'thumbnail-document': 'Document Thumbnail',
    'thumbnail-image': 'Image Thumbnail',
    'thumbnail-video': 'Video Thumbnail',
    'thumbnail-link': 'Link Thumbnail',
    'md-msg-hist': 'History',
    'md-app-state': 'App State',
    'product-catalog-image': '',
    'payment-bg-image': 'Payment Background',
    'ptv': 'Video'
};
exports.MEDIA_KEYS = Object.keys(exports.MEDIA_PATH_MAP);
exports.MIN_PREKEY_COUNT = 5;
exports.INITIAL_PREKEY_COUNT = 30;
exports.DEFAULT_CACHE_TTLS = {
    SIGNAL_STORE: 5 * 60,
    MSG_RETRY: 60 * 60,
    CALL_OFFER: 5 * 60,
    USER_DEVICES: 5 * 60, // 5 minutes
};
