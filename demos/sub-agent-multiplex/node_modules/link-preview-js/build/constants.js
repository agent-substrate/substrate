"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CONSTANTS = void 0;
exports.CONSTANTS = {
    REGEX_VALID_URL: new RegExp("^" +
        // protocol identifier
        "(?:(?:https?|ftp)://)" +
        // user:pass authentication
        "(?:\\S+(?::\\S*)?@)?" +
        "(?:" +
        // IP address exclusion
        // private & local networks
        "(?!(?:10|127)(?:\\.\\d{1,3}){3})" +
        "(?!(?:169\\.254|192\\.168)(?:\\.\\d{1,3}){2})" +
        "(?!172\\.(?:1[6-9]|2\\d|3[0-1])(?:\\.\\d{1,3}){2})" +
        // IP address dotted notation octets
        // excludes loopback network 0.0.0.0
        // excludes reserved space >= 224.0.0.0
        // excludes network & broacast addresses
        // (first & last IP address of each class)
        "(?:[1-9]\\d?|1\\d\\d|2[01]\\d|22[0-3])" +
        "(?:\\.(?:1?\\d{1,2}|2[0-4]\\d|25[0-5])){2}" +
        "(?:\\.(?:[1-9]\\d?|1\\d\\d|2[0-4]\\d|25[0-4]))" +
        "|" +
        // host name
        "(?:(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)" +
        // domain name
        "(?:\\.(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)*" +
        // TLD identifier
        "(?:\\.(?:[a-z\\u00a1-\\uffff]{2,}))" +
        // TLD may end with dot
        "\\.?" +
        ")" +
        // port number
        "(?::\\d{2,5})?" +
        // resource path
        "(?:[/?#]\\S*)?" +
        "$", "i"),
    REGEX_LOOPBACK: new RegExp("^" +
        // Loopback: 127.0.0.0 - 127.255.255.255
        "(?:127(?:\\.\\d{1,3}){3})" +
        "|" +
        // Private Class A: 10.0.0.0 - 10.255.255.255
        "(?:10(?:\\.\\d{1,3}){3})" +
        "|" +
        // Private Class B: 172.16.0.0 - 172.31.255.255
        "(?:172\\.(?:1[6-9]|2\\d|3[0-1])(?:\\.\\d{1,3}){2})" +
        "|" +
        // Private Class C: 192.168.0.0 - 192.168.255.255
        "(?:192\\.168(?:\\.\\d{1,3}){2})" +
        "|" +
        // Link-local: 169.254.0.0 - 169.254.255.255
        "(?:169\\.254(?:\\.\\d{1,3}){2})" +
        "|" +
        // Documentation: 192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24
        "(?:192\\.0\\.2(?:\\.\\d{1,3}){1})" +
        "|" +
        "(?:198\\.51\\.100(?:\\.\\d{1,3}){1})" +
        "|" +
        "(?:203\\.0\\.113(?:\\.\\d{1,3}){1})" +
        "|" +
        // Carrier-Grade NAT (CGNAT): 100.64.0.0 - 100.127.255.255
        "(?:100\\.(?:6[4-9]|[7-9]\\d|1[0-1]\\d)(?:\\.\\d{1,3}){2})" +
        "$", "i"),
    REGEX_CONTENT_TYPE_IMAGE: new RegExp("image/.*", "i"),
    REGEX_CONTENT_TYPE_AUDIO: new RegExp("audio/.*", "i"),
    REGEX_CONTENT_TYPE_VIDEO: new RegExp("video/.*", "i"),
    REGEX_CONTENT_TYPE_TEXT: new RegExp("text/.*", "i"),
    REGEX_CONTENT_TYPE_APPLICATION: new RegExp("application/.*", "i"),
};
