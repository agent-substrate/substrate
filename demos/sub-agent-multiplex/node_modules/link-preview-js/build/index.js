"use strict";
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getPreviewFromContent = exports.getLinkPreview = void 0;
var cheerio_1 = __importDefault(require("cheerio"));
var url_1 = __importDefault(require("url"));
var constants_1 = require("./constants");
;
function throwOnLoopback(address) {
    if (constants_1.CONSTANTS.REGEX_LOOPBACK.test(address)) {
        throw new Error("SSRF request detected, trying to query host");
    }
}
function metaTag(doc, type, attr) {
    var nodes = doc("meta[".concat(attr, "='").concat(type, "']"));
    return nodes.length ? nodes : null;
}
function metaTagContent(doc, type, attr) {
    return doc("meta[".concat(attr, "='").concat(type, "']")).attr("content");
}
function getTitle(doc) {
    var title = metaTagContent(doc, "og:title", "property") ||
        metaTagContent(doc, "og:title", "name");
    if (!title) {
        title = doc("head > title").text();
    }
    return title;
}
function getSiteName(doc) {
    var siteName = metaTagContent(doc, "og:site_name", "property") ||
        metaTagContent(doc, "og:site_name", "name");
    return siteName;
}
function getAuthor(doc) {
    var author = metaTagContent(doc, "author", "name") ||
        metaTagContent(doc, "article:author", "property");
    return author;
}
function getDescription(doc) {
    var description = metaTagContent(doc, "description", "name") ||
        metaTagContent(doc, "Description", "name") ||
        metaTagContent(doc, "og:description", "property");
    return description;
}
function getMediaType(doc) {
    var node = metaTag(doc, "medium", "name");
    if (node) {
        var content = node.attr("content");
        return content === "image" ? "photo" : content;
    }
    return (metaTagContent(doc, "og:type", "property") ||
        metaTagContent(doc, "og:type", "name"));
}
function getImages(doc, rootUrl, imagesPropertyType) {
    var images = [];
    var nodes;
    var src;
    var dic = {};
    var imagePropertyType = imagesPropertyType !== null && imagesPropertyType !== void 0 ? imagesPropertyType : "og";
    nodes =
        metaTag(doc, "".concat(imagePropertyType, ":image"), "property") ||
            metaTag(doc, "".concat(imagePropertyType, ":image"), "name");
    if (nodes) {
        nodes.each(function (_, node) {
            if (node.type === "tag") {
                src = node.attribs.content;
                if (src) {
                    src = url_1.default.resolve(rootUrl, src);
                    images.push(src);
                }
            }
        });
    }
    if (images.length <= 0 && !imagesPropertyType) {
        src = doc("link[rel=image_src]").attr("href");
        if (src) {
            src = url_1.default.resolve(rootUrl, src);
            images = [src];
        }
        else {
            nodes = doc("img");
            if (nodes === null || nodes === void 0 ? void 0 : nodes.length) {
                dic = {};
                images = [];
                nodes.each(function (_, node) {
                    if (node.type === "tag")
                        src = node.attribs.src;
                    if (src && !dic[src]) {
                        dic[src] = true;
                        // width = node.attribs.width;
                        // height = node.attribs.height;
                        images.push(url_1.default.resolve(rootUrl, src));
                    }
                });
            }
        }
    }
    return images;
}
function getVideos(doc) {
    var videos = [];
    var nodeTypes;
    var nodeSecureUrls;
    var nodeType;
    var nodeSecureUrl;
    var video;
    var videoType;
    var videoSecureUrl;
    var width;
    var height;
    var videoObj;
    var index;
    var nodes = metaTag(doc, "og:video", "property") || metaTag(doc, "og:video", "name");
    if (nodes === null || nodes === void 0 ? void 0 : nodes.length) {
        nodeTypes =
            metaTag(doc, "og:video:type", "property") ||
                metaTag(doc, "og:video:type", "name");
        nodeSecureUrls =
            metaTag(doc, "og:video:secure_url", "property") ||
                metaTag(doc, "og:video:secure_url", "name");
        width =
            metaTagContent(doc, "og:video:width", "property") ||
                metaTagContent(doc, "og:video:width", "name");
        height =
            metaTagContent(doc, "og:video:height", "property") ||
                metaTagContent(doc, "og:video:height", "name");
        for (index = 0; index < nodes.length; index += 1) {
            var node = nodes[index];
            if (node.type === "tag")
                video = node.attribs.content;
            nodeType = nodeTypes === null || nodeTypes === void 0 ? void 0 : nodeTypes[index];
            if ((nodeType === null || nodeType === void 0 ? void 0 : nodeType.type) === "tag") {
                videoType = nodeType ? nodeType.attribs.content : null;
            }
            nodeSecureUrl = nodeSecureUrls === null || nodeSecureUrls === void 0 ? void 0 : nodeSecureUrls[index];
            if ((nodeSecureUrl === null || nodeSecureUrl === void 0 ? void 0 : nodeSecureUrl.type) === "tag") {
                videoSecureUrl = nodeSecureUrl ? nodeSecureUrl.attribs.content : null;
            }
            videoObj = {
                url: video,
                secureUrl: videoSecureUrl,
                type: videoType,
                width: width,
                height: height,
            };
            if (videoType && videoType.indexOf("video/") === 0) {
                videos.splice(0, 0, videoObj);
            }
            else {
                videos.push(videoObj);
            }
        }
    }
    return videos;
}
// returns default favicon (//hostname/favicon.ico) for a url
function getDefaultFavicon(rootUrl) {
    return url_1.default.resolve(rootUrl, "/favicon.ico");
}
// returns an array of URLs to favicon images
function getFavicons(doc, rootUrl) {
    var images = [];
    var nodes = [];
    var src;
    var relSelectors = [
        "rel=icon",
        "rel=\"shortcut icon\"",
        "rel=apple-touch-icon",
    ];
    relSelectors.forEach(function (relSelector) {
        // look for all icon tags
        nodes = doc("link[".concat(relSelector, "]"));
        // collect all images from icon tags
        if (nodes.length) {
            nodes.each(function (_, node) {
                if (node.type === "tag")
                    src = node.attribs.href;
                if (src) {
                    src = url_1.default.resolve(rootUrl, src);
                    images.push(src);
                }
            });
        }
    });
    // if no icon images, use default favicon location
    if (images.length <= 0) {
        images.push(getDefaultFavicon(rootUrl));
    }
    return images;
}
function parseImageResponse(url, contentType) {
    return {
        url: url,
        mediaType: "image",
        contentType: contentType,
        favicons: [getDefaultFavicon(url)],
    };
}
function parseAudioResponse(url, contentType) {
    return {
        url: url,
        mediaType: "audio",
        contentType: contentType,
        favicons: [getDefaultFavicon(url)],
    };
}
function parseVideoResponse(url, contentType) {
    return {
        url: url,
        mediaType: "video",
        contentType: contentType,
        favicons: [getDefaultFavicon(url)],
    };
}
function parseApplicationResponse(url, contentType) {
    return {
        url: url,
        mediaType: "application",
        contentType: contentType,
        favicons: [getDefaultFavicon(url)],
    };
}
function parseTextResponse(body, url, options, contentType) {
    if (options === void 0) { options = {}; }
    var doc = cheerio_1.default.load(body);
    var response = {
        url: url,
        title: getTitle(doc),
        siteName: getSiteName(doc),
        description: getDescription(doc),
        author: getAuthor(doc),
        mediaType: getMediaType(doc) || "website",
        contentType: contentType,
        images: getImages(doc, url, options.imagesPropertyType),
        videos: getVideos(doc),
        favicons: getFavicons(doc, url),
    };
    if ((options === null || options === void 0 ? void 0 : options.onResponse) && typeof options.onResponse !== "function") {
        throw new Error("link-preview-js onResponse option must be a function");
    }
    if (options === null || options === void 0 ? void 0 : options.onResponse) {
        // send in a cloned response (to avoid mutation of original response reference)
        var clonedResponse = structuredClone(response);
        var urlObject = new URL(url);
        response = options.onResponse(clonedResponse, doc, urlObject);
    }
    return response;
}
function parseUnknownResponse(body, url, options, contentType) {
    if (options === void 0) { options = {}; }
    return parseTextResponse(body, url, options, contentType);
}
function parseResponse(response, options) {
    try {
        // console.log("[link-preview-js] response", response);
        var contentType = response.headers["content-type"];
        var contentTypeTokens = [];
        var charset = null;
        if (!contentType) {
            return parseUnknownResponse(response.data, response.url, options);
        }
        if (contentType.includes(";")) {
            contentTypeTokens = contentType.split(";");
            contentType = contentTypeTokens[0];
            for (var _i = 0, contentTypeTokens_1 = contentTypeTokens; _i < contentTypeTokens_1.length; _i++) {
                var token = contentTypeTokens_1[_i];
                if (token.indexOf("charset=") !== -1) {
                    charset = token.split("=")[1];
                }
            }
        }
        // parse response depending on content type
        if (constants_1.CONSTANTS.REGEX_CONTENT_TYPE_IMAGE.test(contentType)) {
            return __assign(__assign({}, parseImageResponse(response.url, contentType)), { charset: charset });
        }
        if (constants_1.CONSTANTS.REGEX_CONTENT_TYPE_AUDIO.test(contentType)) {
            return __assign(__assign({}, parseAudioResponse(response.url, contentType)), { charset: charset });
        }
        if (constants_1.CONSTANTS.REGEX_CONTENT_TYPE_VIDEO.test(contentType)) {
            return __assign(__assign({}, parseVideoResponse(response.url, contentType)), { charset: charset });
        }
        if (constants_1.CONSTANTS.REGEX_CONTENT_TYPE_TEXT.test(contentType)) {
            var htmlString_1 = response.data;
            return __assign(__assign({}, parseTextResponse(htmlString_1, response.url, options, contentType)), { charset: charset });
        }
        if (constants_1.CONSTANTS.REGEX_CONTENT_TYPE_APPLICATION.test(contentType)) {
            return __assign(__assign({}, parseApplicationResponse(response.url, contentType)), { charset: charset });
        }
        var htmlString = response.data;
        return __assign(__assign({}, parseUnknownResponse(htmlString, response.url, options)), { charset: charset });
    }
    catch (e) {
        throw new Error("link-preview-js could not fetch link information ".concat(e.toString()));
    }
}
/**
 * Parses the text, extracts the first link it finds and does a HTTP request
 * to fetch the website content, afterwards it tries to parse the internal HTML
 * and extract the information via meta tags
 * @param text string, text to be parsed
 * @param options ILinkPreviewOptions
 */
function getLinkPreview(text, options) {
    var _a, _b, _c;
    return __awaiter(this, void 0, void 0, function () {
        var detectedUrl, resolvedUrl, timeout, controller, timeoutCounter, fetchOptions, fetchUrl, response, locationHeader, isAbsoluteURI, forwardedUrl, resolvedUrl, headers, normalizedResponse;
        var _d;
        return __generator(this, function (_e) {
            switch (_e.label) {
                case 0:
                    if (!text || typeof text !== "string") {
                        throw new Error("link-preview-js did not receive a valid url or text");
                    }
                    detectedUrl = text
                        .replace(/\n/g, " ")
                        .split(" ")
                        .find(function (token) { return constants_1.CONSTANTS.REGEX_VALID_URL.test(token); });
                    if (!detectedUrl) {
                        throw new Error("link-preview-js did not receive a valid a url or text");
                    }
                    if ((options === null || options === void 0 ? void 0 : options.followRedirects) === "manual" && !(options === null || options === void 0 ? void 0 : options.handleRedirects)) {
                        throw new Error("link-preview-js followRedirects is set to manual, but no handleRedirects function was provided");
                    }
                    if (!!!(options === null || options === void 0 ? void 0 : options.resolveDNSHost)) return [3 /*break*/, 2];
                    return [4 /*yield*/, options.resolveDNSHost(detectedUrl)];
                case 1:
                    resolvedUrl = _e.sent();
                    throwOnLoopback(resolvedUrl);
                    _e.label = 2;
                case 2:
                    timeout = (_a = options === null || options === void 0 ? void 0 : options.timeout) !== null && _a !== void 0 ? _a : 3000;
                    controller = new AbortController();
                    timeoutCounter = setTimeout(function () { return controller.abort(); }, timeout);
                    fetchOptions = {
                        headers: (_b = options === null || options === void 0 ? void 0 : options.headers) !== null && _b !== void 0 ? _b : {},
                        redirect: (_c = options === null || options === void 0 ? void 0 : options.followRedirects) !== null && _c !== void 0 ? _c : "error",
                        signal: controller.signal,
                    };
                    fetchUrl = (options === null || options === void 0 ? void 0 : options.proxyUrl)
                        ? options.proxyUrl.concat(detectedUrl)
                        : detectedUrl;
                    return [4 /*yield*/, fetch(fetchUrl, fetchOptions).catch(function (e) {
                            if (e.name === "AbortError") {
                                throw new Error("Request timeout");
                            }
                            clearTimeout(timeoutCounter);
                            throw e;
                        })];
                case 3:
                    response = _e.sent();
                    if (!(response.status > 300 &&
                        response.status < 309 &&
                        fetchOptions.redirect === "manual" &&
                        (options === null || options === void 0 ? void 0 : options.handleRedirects))) return [3 /*break*/, 7];
                    locationHeader = response.headers.get("location") || "";
                    isAbsoluteURI = locationHeader.startsWith('http://') || locationHeader.startsWith('https://');
                    forwardedUrl = isAbsoluteURI
                        ? locationHeader
                        : url_1.default.resolve(fetchUrl, locationHeader);
                    if (!options.handleRedirects(fetchUrl, forwardedUrl)) {
                        throw new Error("link-preview-js could not handle redirect");
                    }
                    if (!!!(options === null || options === void 0 ? void 0 : options.resolveDNSHost)) return [3 /*break*/, 5];
                    return [4 /*yield*/, options.resolveDNSHost(forwardedUrl)];
                case 4:
                    resolvedUrl = _e.sent();
                    throwOnLoopback(resolvedUrl);
                    _e.label = 5;
                case 5: return [4 /*yield*/, fetch(forwardedUrl, fetchOptions)];
                case 6:
                    response = _e.sent();
                    _e.label = 7;
                case 7:
                    clearTimeout(timeoutCounter);
                    headers = {};
                    response.headers.forEach(function (header, key) {
                        headers[key] = header;
                    });
                    _d = {
                        url: (options === null || options === void 0 ? void 0 : options.proxyUrl)
                            ? response.url.replace(options.proxyUrl, "")
                            : response.url,
                        headers: headers
                    };
                    return [4 /*yield*/, response.text()];
                case 8:
                    normalizedResponse = (_d.data = _e.sent(),
                        _d);
                    return [2 /*return*/, parseResponse(normalizedResponse, options)];
            }
        });
    });
}
exports.getLinkPreview = getLinkPreview;
/**
 * Skip the library fetching the website for you, instead pass a response object
 * from whatever source you get and use the internal parsing of the HTML to return
 * the necessary information
 * @param response Preview Response
 * @param options IPreviewLinkOptions
 */
function getPreviewFromContent(response, options) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            if (!response || typeof response !== "object") {
                throw new Error("link-preview-js did not receive a valid response object");
            }
            if (!response.url) {
                throw new Error("link-preview-js did not receive a valid response object");
            }
            return [2 /*return*/, parseResponse(response, options)];
        });
    });
}
exports.getPreviewFromContent = getPreviewFromContent;
