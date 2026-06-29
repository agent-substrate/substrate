"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = validatePhoneNumberLength;
var _normalizeArguments2 = _interopRequireDefault(require("./normalizeArguments.js"));
var _parsePhoneNumberWithError_ = _interopRequireDefault(require("./parsePhoneNumberWithError_.js"));
var _ParseError = _interopRequireDefault(require("./ParseError.js"));
var _metadata = _interopRequireDefault(require("./metadata.js"));
var _checkNumberLength = _interopRequireDefault(require("./helpers/checkNumberLength.js"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
function _typeof(o) { "@babel/helpers - typeof"; return _typeof = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function (o) { return typeof o; } : function (o) { return o && "function" == typeof Symbol && o.constructor === Symbol && o !== Symbol.prototype ? "symbol" : typeof o; }, _typeof(o); }
function ownKeys(e, r) { var t = Object.keys(e); if (Object.getOwnPropertySymbols) { var o = Object.getOwnPropertySymbols(e); r && (o = o.filter(function (r) { return Object.getOwnPropertyDescriptor(e, r).enumerable; })), t.push.apply(t, o); } return t; }
function _objectSpread(e) { for (var r = 1; r < arguments.length; r++) { var t = null != arguments[r] ? arguments[r] : {}; r % 2 ? ownKeys(Object(t), !0).forEach(function (r) { _defineProperty(e, r, t[r]); }) : Object.getOwnPropertyDescriptors ? Object.defineProperties(e, Object.getOwnPropertyDescriptors(t)) : ownKeys(Object(t)).forEach(function (r) { Object.defineProperty(e, r, Object.getOwnPropertyDescriptor(t, r)); }); } return e; }
function _defineProperty(e, r, t) { return (r = _toPropertyKey(r)) in e ? Object.defineProperty(e, r, { value: t, enumerable: !0, configurable: !0, writable: !0 }) : e[r] = t, e; }
function _toPropertyKey(t) { var i = _toPrimitive(t, "string"); return "symbol" == _typeof(i) ? i : i + ""; }
function _toPrimitive(t, r) { if ("object" != _typeof(t) || !t) return t; var e = t[Symbol.toPrimitive]; if (void 0 !== e) { var i = e.call(t, r || "default"); if ("object" != _typeof(i)) return i; throw new TypeError("@@toPrimitive must return a primitive value."); } return ("string" === r ? String : Number)(t); }
function validatePhoneNumberLength() {
  var _normalizeArguments = (0, _normalizeArguments2["default"])(arguments),
    text = _normalizeArguments.text,
    options = _normalizeArguments.options,
    metadata = _normalizeArguments.metadata;
  options = _objectSpread(_objectSpread({}, options), {}, {
    extract: false
  });

  // Parse phone number.
  try {
    var phoneNumber = (0, _parsePhoneNumberWithError_["default"])(text, options, metadata);
    var metadataInstance = new _metadata["default"](metadata);
    metadataInstance.selectNumberingPlan(phoneNumber.country || phoneNumber.countryCallingCode);
    var result = (0, _checkNumberLength["default"])(phoneNumber.nationalNumber, undefined, metadataInstance);
    if (result !== 'IS_POSSIBLE') {
      return result;
    }
  } catch (error) {
    /* istanbul ignore else */
    if (error instanceof _ParseError["default"]) {
      return error.message;
    } else {
      throw error;
    }
  }
}
//# sourceMappingURL=validatePhoneNumberLength.js.map