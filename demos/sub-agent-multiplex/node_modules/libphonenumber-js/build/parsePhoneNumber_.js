"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = parsePhoneNumber;
var _parsePhoneNumberWithError_ = _interopRequireDefault(require("./parsePhoneNumberWithError_.js"));
var _ParseError = _interopRequireDefault(require("./ParseError.js"));
var _metadata = require("./metadata.js");
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
function _typeof(o) { "@babel/helpers - typeof"; return _typeof = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function (o) { return typeof o; } : function (o) { return o && "function" == typeof Symbol && o.constructor === Symbol && o !== Symbol.prototype ? "symbol" : typeof o; }, _typeof(o); }
function ownKeys(e, r) { var t = Object.keys(e); if (Object.getOwnPropertySymbols) { var o = Object.getOwnPropertySymbols(e); r && (o = o.filter(function (r) { return Object.getOwnPropertyDescriptor(e, r).enumerable; })), t.push.apply(t, o); } return t; }
function _objectSpread(e) { for (var r = 1; r < arguments.length; r++) { var t = null != arguments[r] ? arguments[r] : {}; r % 2 ? ownKeys(Object(t), !0).forEach(function (r) { _defineProperty(e, r, t[r]); }) : Object.getOwnPropertyDescriptors ? Object.defineProperties(e, Object.getOwnPropertyDescriptors(t)) : ownKeys(Object(t)).forEach(function (r) { Object.defineProperty(e, r, Object.getOwnPropertyDescriptor(t, r)); }); } return e; }
function _defineProperty(e, r, t) { return (r = _toPropertyKey(r)) in e ? Object.defineProperty(e, r, { value: t, enumerable: !0, configurable: !0, writable: !0 }) : e[r] = t, e; }
function _toPropertyKey(t) { var i = _toPrimitive(t, "string"); return "symbol" == _typeof(i) ? i : i + ""; }
function _toPrimitive(t, r) { if ("object" != _typeof(t) || !t) return t; var e = t[Symbol.toPrimitive]; if (void 0 !== e) { var i = e.call(t, r || "default"); if ("object" != _typeof(i)) return i; throw new TypeError("@@toPrimitive must return a primitive value."); } return ("string" === r ? String : Number)(t); }
function parsePhoneNumber(text, options, metadata) {
  // Validate `defaultCountry`.
  if (options && options.defaultCountry && !(0, _metadata.isSupportedCountry)(options.defaultCountry, metadata)) {
    options = _objectSpread(_objectSpread({}, options), {}, {
      defaultCountry: undefined
    });
  }
  // Parse phone number.
  try {
    return (0, _parsePhoneNumberWithError_["default"])(text, options, metadata);
  } catch (error) {
    /* istanbul ignore else */
    if (error instanceof _ParseError["default"]) {
      //
    } else {
      throw error;
    }
  }
}
//# sourceMappingURL=parsePhoneNumber_.js.map