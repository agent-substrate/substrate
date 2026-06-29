"use strict";

function _typeof(o) { "@babel/helpers - typeof"; return _typeof = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function (o) { return typeof o; } : function (o) { return o && "function" == typeof Symbol && o.constructor === Symbol && o !== Symbol.prototype ? "symbol" : typeof o; }, _typeof(o); }
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = findPhoneNumbers;
exports.searchPhoneNumbers = searchPhoneNumbers;
var _findPhoneNumbersInitialImplementation = _interopRequireWildcard(require("./findPhoneNumbersInitialImplementation.js"));
var _normalizeArguments3 = _interopRequireDefault(require("../normalizeArguments.js"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
function _interopRequireWildcard(e, t) { if ("function" == typeof WeakMap) var r = new WeakMap(), n = new WeakMap(); return (_interopRequireWildcard = function _interopRequireWildcard(e, t) { if (!t && e && e.__esModule) return e; var o, i, f = { __proto__: null, "default": e }; if (null === e || "object" != _typeof(e) && "function" != typeof e) return f; if (o = t ? n : r) { if (o.has(e)) return o.get(e); o.set(e, f); } for (var _t in e) "default" !== _t && {}.hasOwnProperty.call(e, _t) && ((i = (o = Object.defineProperty) && Object.getOwnPropertyDescriptor(e, _t)) && (i.get || i.set) ? o(f, _t, i) : f[_t] = e[_t]); return f; })(e, t); }
// This is a legacy function.
// Use `findNumbers()` instead.

function findPhoneNumbers() {
  var _normalizeArguments = (0, _normalizeArguments3["default"])(arguments),
    text = _normalizeArguments.text,
    options = _normalizeArguments.options,
    metadata = _normalizeArguments.metadata;
  return (0, _findPhoneNumbersInitialImplementation["default"])(text, options, metadata);
}

/**
 * @return ES6 `for ... of` iterator.
 */
function searchPhoneNumbers() {
  var _normalizeArguments2 = (0, _normalizeArguments3["default"])(arguments),
    text = _normalizeArguments2.text,
    options = _normalizeArguments2.options,
    metadata = _normalizeArguments2.metadata;
  return (0, _findPhoneNumbersInitialImplementation.searchPhoneNumbers)(text, options, metadata);
}
//# sourceMappingURL=findPhoneNumbers.js.map