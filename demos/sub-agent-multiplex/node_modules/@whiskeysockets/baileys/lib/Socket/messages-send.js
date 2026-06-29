"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.makeMessagesSocket = void 0;
const boom_1 = require("@hapi/boom");
const node_cache_1 = __importDefault(require("node-cache"));
const WAProto_1 = require("../../WAProto");
const Defaults_1 = require("../Defaults");
const Utils_1 = require("../Utils");
const link_preview_1 = require("../Utils/link-preview");
const WABinary_1 = require("../WABinary");
const groups_1 = require("./groups");
var ListType = WAProto_1.proto.Message.ListMessage.ListType;
const makeMessagesSocket = (config) => {
    const { logger, linkPreviewImageThumbnailWidth, generateHighQualityLinkPreview, options: axiosOptions, patchMessageBeforeSending, } = config;
    const sock = (0, groups_1.makeGroupsSocket)(config);
    const { ev, authState, processingMutex, signalRepository, upsertMessage, query, fetchPrivacySettings, generateMessageTag, sendNode, groupMetadata, groupToggleEphemeral } = sock;
    const userDevicesCache = config.userDevicesCache || new node_cache_1.default({
        stdTTL: Defaults_1.DEFAULT_CACHE_TTLS.USER_DEVICES,
        useClones: false
    });
    let mediaConn;
    const refreshMediaConn = async (forceGet = false) => {
        const media = await mediaConn;
        if (!media || forceGet || (new Date().getTime() - media.fetchDate.getTime()) > media.ttl * 1000) {
            mediaConn = (async () => {
                const result = await query({
                    tag: 'iq',
                    attrs: {
                        type: 'set',
                        xmlns: 'w:m',
                        to: WABinary_1.S_WHATSAPP_NET,
                    },
                    content: [{ tag: 'media_conn', attrs: {} }]
                });
                const mediaConnNode = (0, WABinary_1.getBinaryNodeChild)(result, 'media_conn');
                const node = {
                    hosts: (0, WABinary_1.getBinaryNodeChildren)(mediaConnNode, 'host').map(({ attrs }) => ({
                        hostname: attrs.hostname,
                        maxContentLengthBytes: +attrs.maxContentLengthBytes,
                    })),
                    auth: mediaConnNode.attrs.auth,
                    ttl: +mediaConnNode.attrs.ttl,
                    fetchDate: new Date()
                };
                logger.debug('fetched media conn');
                return node;
            })();
        }
        return mediaConn;
    };
    /**
     * generic send receipt function
     * used for receipts of phone call, read, delivery etc.
     * */
    const sendReceipt = async (jid, participant, messageIds, type) => {
        const node = {
            tag: 'receipt',
            attrs: {
                id: messageIds[0],
            },
        };
        const isReadReceipt = type === 'read' || type === 'read-self';
        if (isReadReceipt) {
            node.attrs.t = (0, Utils_1.unixTimestampSeconds)().toString();
        }
        if (type === 'sender' && (0, WABinary_1.isJidUser)(jid)) {
            node.attrs.recipient = jid;
            node.attrs.to = participant;
        }
        else {
            node.attrs.to = jid;
            if (participant) {
                node.attrs.participant = participant;
            }
        }
        if (type) {
            node.attrs.type = type;
        }
        const remainingMessageIds = messageIds.slice(1);
        if (remainingMessageIds.length) {
            node.content = [
                {
                    tag: 'list',
                    attrs: {},
                    content: remainingMessageIds.map(id => ({
                        tag: 'item',
                        attrs: { id }
                    }))
                }
            ];
        }
        logger.debug({ attrs: node.attrs, messageIds }, 'sending receipt for messages');
        await sendNode(node);
    };
    /** Correctly bulk send receipts to multiple chats, participants */
    const sendReceipts = async (keys, type) => {
        const recps = (0, Utils_1.aggregateMessageKeysNotFromMe)(keys);
        for (const { jid, participant, messageIds } of recps) {
            await sendReceipt(jid, participant, messageIds, type);
        }
    };
    /** Bulk read messages. Keys can be from different chats & participants */
    const readMessages = async (keys) => {
        const privacySettings = await fetchPrivacySettings();
        // based on privacy settings, we have to change the read type
        const readType = privacySettings.readreceipts === 'all' ? 'read' : 'read-self';
        await sendReceipts(keys, readType);
    };
    /** Fetch all the devices we've to send a message to */
    const getUSyncDevices = async (jids, useCache, ignoreZeroDevices) => {
        var _a;
        const deviceResults = [];
        if (!useCache) {
            logger.debug('not using cache for devices');
        }
        const users = [];
        jids = Array.from(new Set(jids));
        for (let jid of jids) {
            const user = (_a = (0, WABinary_1.jidDecode)(jid)) === null || _a === void 0 ? void 0 : _a.user;
            jid = (0, WABinary_1.jidNormalizedUser)(jid);
            const devices = userDevicesCache.get(user);
            if (devices && useCache) {
                deviceResults.push(...devices);
                logger.trace({ user }, 'using cache for devices');
            }
            else {
                users.push({ tag: 'user', attrs: { jid } });
            }
        }
        const iq = {
            tag: 'iq',
            attrs: {
                to: WABinary_1.S_WHATSAPP_NET,
                type: 'get',
                xmlns: 'usync',
            },
            content: [
                {
                    tag: 'usync',
                    attrs: {
                        sid: generateMessageTag(),
                        mode: 'query',
                        last: 'true',
                        index: '0',
                        context: 'message',
                    },
                    content: [
                        {
                            tag: 'query',
                            attrs: {},
                            content: [
                                {
                                    tag: 'devices',
                                    attrs: { version: '2' }
                                }
                            ]
                        },
                        { tag: 'list', attrs: {}, content: users }
                    ]
                },
            ],
        };
        const result = await query(iq);
        const extracted = (0, Utils_1.extractDeviceJids)(result, authState.creds.me.id, ignoreZeroDevices);
        const deviceMap = {};
        for (const item of extracted) {
            deviceMap[item.user] = deviceMap[item.user] || [];
            deviceMap[item.user].push(item);
            deviceResults.push(item);
        }
        for (const key in deviceMap) {
            userDevicesCache.set(key, deviceMap[key]);
        }
        return deviceResults;
    };
    const assertSessions = async (jids, force) => {
        let didFetchNewSession = false;
        let jidsRequiringFetch = [];
        if (force) {
            jidsRequiringFetch = jids;
        }
        else {
            const addrs = jids.map(jid => (signalRepository
                .jidToSignalProtocolAddress(jid)));
            const sessions = await authState.keys.get('session', addrs);
            for (const jid of jids) {
                const signalId = signalRepository
                    .jidToSignalProtocolAddress(jid);
                if (!sessions[signalId]) {
                    jidsRequiringFetch.push(jid);
                }
            }
        }
        if (jidsRequiringFetch.length) {
            logger.debug({ jidsRequiringFetch }, 'fetching sessions');
            const result = await query({
                tag: 'iq',
                attrs: {
                    xmlns: 'encrypt',
                    type: 'get',
                    to: WABinary_1.S_WHATSAPP_NET,
                },
                content: [
                    {
                        tag: 'key',
                        attrs: {},
                        content: jidsRequiringFetch.map(jid => ({
                            tag: 'user',
                            attrs: { jid },
                        }))
                    }
                ]
            });
            await (0, Utils_1.parseAndInjectE2ESessions)(result, signalRepository);
            didFetchNewSession = true;
        }
        return didFetchNewSession;
    };
    const createParticipantNodes = async (jids, message, extraAttrs) => {
        const patched = await patchMessageBeforeSending(message, jids);
        const bytes = (0, Utils_1.encodeWAMessage)(patched);
        let shouldIncludeDeviceIdentity = false;
        const nodes = await Promise.all(jids.map(async (jid) => {
            const { type, ciphertext } = await signalRepository
                .encryptMessage({ jid, data: bytes });
            if (type === 'pkmsg') {
                shouldIncludeDeviceIdentity = true;
            }
            const node = {
                tag: 'to',
                attrs: { jid },
                content: [{
                        tag: 'enc',
                        attrs: {
                            v: '2',
                            type,
                            ...extraAttrs || {}
                        },
                        content: ciphertext
                    }]
            };
            return node;
        }));
        return { nodes, shouldIncludeDeviceIdentity };
    };
    const relayMessage = async (jid, message, { messageId: msgId, participant, additionalAttributes, useUserDevicesCache, cachedGroupMetadata, statusJidList }) => {
        const meId = authState.creds.me.id;
        let shouldIncludeDeviceIdentity = false;
        const { user, server } = (0, WABinary_1.jidDecode)(jid);
        const statusJid = 'status@broadcast';
        const isGroup = server === 'g.us';
        const isStatus = jid === statusJid;
        const isLid = server === 'lid';
        msgId = msgId || (0, Utils_1.generateMessageID)();
        useUserDevicesCache = useUserDevicesCache !== false;
        const participants = [];
        const destinationJid = (!isStatus) ? (0, WABinary_1.jidEncode)(user, isLid ? 'lid' : isGroup ? 'g.us' : 's.whatsapp.net') : statusJid;
        const binaryNodeContent = [];
        const devices = [];
        const meMsg = {
            deviceSentMessage: {
                destinationJid,
                message
            }
        };
        if (participant) {
            // when the retry request is not for a group
            // only send to the specific device that asked for a retry
            // otherwise the message is sent out to every device that should be a recipient
            if (!isGroup && !isStatus) {
                additionalAttributes = { ...additionalAttributes, 'device_fanout': 'false' };
            }
            const { user, device } = (0, WABinary_1.jidDecode)(participant.jid);
            devices.push({ user, device });
        }
        await authState.keys.transaction(async () => {
            var _a, _b;
            const mediaType = getMediaType(message);
            if (isGroup || isStatus) {
                const [groupData, senderKeyMap] = await Promise.all([
                    (async () => {
                        let groupData = cachedGroupMetadata ? await cachedGroupMetadata(jid) : undefined;
                        if (groupData) {
                            logger.trace({ jid, participants: groupData.participants.length }, 'using cached group metadata');
                        }
                        if (!groupData && !isStatus) {
                            groupData = await groupMetadata(jid);
                        }
                        return groupData;
                    })(),
                    (async () => {
                        if (!participant && !isStatus) {
                            const result = await authState.keys.get('sender-key-memory', [jid]);
                            return result[jid] || {};
                        }
                        return {};
                    })()
                ]);
                if (!participant) {
                    const participantsList = (groupData && !isStatus) ? groupData.participants.map(p => p.id) : [];
                    if (isStatus && statusJidList) {
                        participantsList.push(...statusJidList);
                    }
                    const additionalDevices = await getUSyncDevices(participantsList, !!useUserDevicesCache, false);
                    devices.push(...additionalDevices);
                }
                const patched = await patchMessageBeforeSending(message, devices.map(d => (0, WABinary_1.jidEncode)(d.user, isLid ? 'lid' : 's.whatsapp.net', d.device)));
                const bytes = (0, Utils_1.encodeWAMessage)(patched);
                const { ciphertext, senderKeyDistributionMessage } = await signalRepository.encryptGroupMessage({
                    group: destinationJid,
                    data: bytes,
                    meId,
                });
                const senderKeyJids = [];
                // ensure a connection is established with every device
                for (const { user, device } of devices) {
                    const jid = (0, WABinary_1.jidEncode)(user, isLid ? 'lid' : 's.whatsapp.net', device);
                    if (!senderKeyMap[jid] || !!participant) {
                        senderKeyJids.push(jid);
                        // store that this person has had the sender keys sent to them
                        senderKeyMap[jid] = true;
                    }
                }
                // if there are some participants with whom the session has not been established
                // if there are, we re-send the senderkey
                if (senderKeyJids.length) {
                    logger.debug({ senderKeyJids }, 'sending new sender key');
                    const senderKeyMsg = {
                        senderKeyDistributionMessage: {
                            axolotlSenderKeyDistributionMessage: senderKeyDistributionMessage,
                            groupId: destinationJid
                        }
                    };
                    await assertSessions(senderKeyJids, false);
                    const result = await createParticipantNodes(senderKeyJids, senderKeyMsg, mediaType ? { mediatype: mediaType } : undefined);
                    shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || result.shouldIncludeDeviceIdentity;
                    participants.push(...result.nodes);
                }
                binaryNodeContent.push({
                    tag: 'enc',
                    attrs: { v: '2', type: 'skmsg' },
                    content: ciphertext
                });
                await authState.keys.set({ 'sender-key-memory': { [jid]: senderKeyMap } });
            }
            else {
                const { user: meUser, device: meDevice } = (0, WABinary_1.jidDecode)(meId);
                if (!participant) {
                    devices.push({ user });
                    // do not send message to self if the device is 0 (mobile)
                    if (meDevice !== undefined && meDevice !== 0) {
                        devices.push({ user: meUser });
                    }
                    const additionalDevices = await getUSyncDevices([meId, jid], !!useUserDevicesCache, true);
                    devices.push(...additionalDevices);
                }
                const allJids = [];
                const meJids = [];
                const otherJids = [];
                for (const { user, device } of devices) {
                    const isMe = user === meUser;
                    const jid = (0, WABinary_1.jidEncode)(isMe && isLid ? ((_b = (_a = authState.creds) === null || _a === void 0 ? void 0 : _a.me) === null || _b === void 0 ? void 0 : _b.lid.split(':')[0]) || user : user, isLid ? 'lid' : 's.whatsapp.net', device);
                    if (isMe) {
                        meJids.push(jid);
                    }
                    else {
                        otherJids.push(jid);
                    }
                    allJids.push(jid);
                }
                await assertSessions(allJids, false);
                const [{ nodes: meNodes, shouldIncludeDeviceIdentity: s1 }, { nodes: otherNodes, shouldIncludeDeviceIdentity: s2 }] = await Promise.all([
                    createParticipantNodes(meJids, meMsg, mediaType ? { mediatype: mediaType } : undefined),
                    createParticipantNodes(otherJids, message, mediaType ? { mediatype: mediaType } : undefined)
                ]);
                participants.push(...meNodes);
                participants.push(...otherNodes);
                shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || s1 || s2;
            }
            if (participants.length) {
                binaryNodeContent.push({
                    tag: 'participants',
                    attrs: {},
                    content: participants
                });
            }
            const stanza = {
                tag: 'message',
                attrs: {
                    id: msgId,
                    type: 'text',
                    ...(additionalAttributes || {})
                },
                content: binaryNodeContent
            };
            // if the participant to send to is explicitly specified (generally retry recp)
            // ensure the message is only sent to that person
            // if a retry receipt is sent to everyone -- it'll fail decryption for everyone else who received the msg
            if (participant) {
                if ((0, WABinary_1.isJidGroup)(destinationJid)) {
                    stanza.attrs.to = destinationJid;
                    stanza.attrs.participant = participant.jid;
                }
                else if ((0, WABinary_1.areJidsSameUser)(participant.jid, meId)) {
                    stanza.attrs.to = participant.jid;
                    stanza.attrs.recipient = destinationJid;
                }
                else {
                    stanza.attrs.to = participant.jid;
                }
            }
            else {
                stanza.attrs.to = destinationJid;
            }
            if (shouldIncludeDeviceIdentity) {
                stanza.content.push({
                    tag: 'device-identity',
                    attrs: {},
                    content: (0, Utils_1.encodeSignedDeviceIdentity)(authState.creds.account, true)
                });
                logger.debug({ jid }, 'adding device identity');
            }
            const buttonType = getButtonType(message);
            if (buttonType) {
                stanza.content.push({
                    tag: 'biz',
                    attrs: {},
                    content: [
                        {
                            tag: buttonType,
                            attrs: getButtonArgs(message),
                        }
                    ]
                });
                logger.debug({ jid }, 'adding business node');
            }
            logger.debug({ msgId }, `sending message to ${participants.length} devices`);
            await sendNode(stanza);
        });
        return msgId;
    };
    const getMediaType = (message) => {
        if (message.imageMessage) {
            return 'image';
        }
        else if (message.videoMessage) {
            return message.videoMessage.gifPlayback ? 'gif' : 'video';
        }
        else if (message.audioMessage) {
            return message.audioMessage.ptt ? 'ptt' : 'audio';
        }
        else if (message.contactMessage) {
            return 'vcard';
        }
        else if (message.documentMessage) {
            return 'document';
        }
        else if (message.contactsArrayMessage) {
            return 'contact_array';
        }
        else if (message.liveLocationMessage) {
            return 'livelocation';
        }
        else if (message.stickerMessage) {
            return 'sticker';
        }
        else if (message.listMessage) {
            return 'list';
        }
        else if (message.listResponseMessage) {
            return 'list_response';
        }
        else if (message.buttonsResponseMessage) {
            return 'buttons_response';
        }
        else if (message.orderMessage) {
            return 'order';
        }
        else if (message.productMessage) {
            return 'product';
        }
        else if (message.interactiveResponseMessage) {
            return 'native_flow_response';
        }
    };
    const getButtonType = (message) => {
        if (message.buttonsMessage) {
            return 'buttons';
        }
        else if (message.buttonsResponseMessage) {
            return 'buttons_response';
        }
        else if (message.interactiveResponseMessage) {
            return 'interactive_response';
        }
        else if (message.listMessage) {
            return 'list';
        }
        else if (message.listResponseMessage) {
            return 'list_response';
        }
    };
    const getButtonArgs = (message) => {
        if (message.templateMessage) {
            // TODO: Add attributes
            return {};
        }
        else if (message.listMessage) {
            const type = message.listMessage.listType;
            if (!type) {
                throw new boom_1.Boom('Expected list type inside message');
            }
            return { v: '2', type: ListType[type].toLowerCase() };
        }
        else {
            return {};
        }
    };
    const getPrivacyTokens = async (jids) => {
        const t = (0, Utils_1.unixTimestampSeconds)().toString();
        const result = await query({
            tag: 'iq',
            attrs: {
                to: WABinary_1.S_WHATSAPP_NET,
                type: 'set',
                xmlns: 'privacy'
            },
            content: [
                {
                    tag: 'tokens',
                    attrs: {},
                    content: jids.map(jid => ({
                        tag: 'token',
                        attrs: {
                            jid: (0, WABinary_1.jidNormalizedUser)(jid),
                            t,
                            type: 'trusted_contact'
                        }
                    }))
                }
            ]
        });
        return result;
    };
    const waUploadToServer = (0, Utils_1.getWAUploadToServer)(config, refreshMediaConn);
    const waitForMsgMediaUpdate = (0, Utils_1.bindWaitForEvent)(ev, 'messages.media-update');
    return {
        ...sock,
        getPrivacyTokens,
        assertSessions,
        relayMessage,
        sendReceipt,
        sendReceipts,
        getButtonArgs,
        readMessages,
        refreshMediaConn,
        waUploadToServer,
        fetchPrivacySettings,
        updateMediaMessage: async (message) => {
            const content = (0, Utils_1.assertMediaContent)(message.message);
            const mediaKey = content.mediaKey;
            const meId = authState.creds.me.id;
            const node = (0, Utils_1.encryptMediaRetryRequest)(message.key, mediaKey, meId);
            let error = undefined;
            await Promise.all([
                sendNode(node),
                waitForMsgMediaUpdate(update => {
                    const result = update.find(c => c.key.id === message.key.id);
                    if (result) {
                        if (result.error) {
                            error = result.error;
                        }
                        else {
                            try {
                                const media = (0, Utils_1.decryptMediaRetryData)(result.media, mediaKey, result.key.id);
                                if (media.result !== WAProto_1.proto.MediaRetryNotification.ResultType.SUCCESS) {
                                    const resultStr = WAProto_1.proto.MediaRetryNotification.ResultType[media.result];
                                    throw new boom_1.Boom(`Media re-upload failed by device (${resultStr})`, { data: media, statusCode: (0, Utils_1.getStatusCodeForMediaRetry)(media.result) || 404 });
                                }
                                content.directPath = media.directPath;
                                content.url = (0, Utils_1.getUrlFromDirectPath)(content.directPath);
                                logger.debug({ directPath: media.directPath, key: result.key }, 'media update successful');
                            }
                            catch (err) {
                                error = err;
                            }
                        }
                        return true;
                    }
                })
            ]);
            if (error) {
                throw error;
            }
            ev.emit('messages.update', [
                { key: message.key, update: { message: message.message } }
            ]);
            return message;
        },
        sendMessage: async (jid, content, options = {}) => {
            var _a, _b;
            const userJid = authState.creds.me.id;
            if (typeof content === 'object' &&
                'disappearingMessagesInChat' in content &&
                typeof content['disappearingMessagesInChat'] !== 'undefined' &&
                (0, WABinary_1.isJidGroup)(jid)) {
                const { disappearingMessagesInChat } = content;
                const value = typeof disappearingMessagesInChat === 'boolean' ?
                    (disappearingMessagesInChat ? Defaults_1.WA_DEFAULT_EPHEMERAL : 0) :
                    disappearingMessagesInChat;
                await groupToggleEphemeral(jid, value);
            }
            else {
                const fullMsg = await (0, Utils_1.generateWAMessage)(jid, content, {
                    logger,
                    userJid,
                    getUrlInfo: text => (0, link_preview_1.getUrlInfo)(text, {
                        thumbnailWidth: linkPreviewImageThumbnailWidth,
                        fetchOpts: {
                            timeout: 3000,
                            ...axiosOptions || {}
                        },
                        logger,
                        uploadImage: generateHighQualityLinkPreview
                            ? waUploadToServer
                            : undefined
                    }),
                    upload: waUploadToServer,
                    mediaCache: config.mediaCache,
                    options: config.options,
                    ...options,
                });
                const isDeleteMsg = 'delete' in content && !!content.delete;
                const isEditMsg = 'edit' in content && !!content.edit;
                const additionalAttributes = {};
                // required for delete
                if (isDeleteMsg) {
                    // if the chat is a group, and I am not the author, then delete the message as an admin
                    if ((0, WABinary_1.isJidGroup)((_a = content.delete) === null || _a === void 0 ? void 0 : _a.remoteJid) && !((_b = content.delete) === null || _b === void 0 ? void 0 : _b.fromMe)) {
                        additionalAttributes.edit = '8';
                    }
                    else {
                        additionalAttributes.edit = '7';
                    }
                }
                else if (isEditMsg) {
                    additionalAttributes.edit = '1';
                }
                await relayMessage(jid, fullMsg.message, { messageId: fullMsg.key.id, cachedGroupMetadata: options.cachedGroupMetadata, additionalAttributes, statusJidList: options.statusJidList });
                if (config.emitOwnEvents) {
                    process.nextTick(() => {
                        processingMutex.mutex(() => (upsertMessage(fullMsg, 'append')));
                    });
                }
                return fullMsg;
            }
        }
    };
};
exports.makeMessagesSocket = makeMessagesSocket;
