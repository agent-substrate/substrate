"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = searchNumbers;
var _normalizeArguments2 = _interopRequireDefault(require("../normalizeArguments.js"));
var _PhoneNumberMatcher = _interopRequireDefault(require("../PhoneNumberMatcher.js"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
function _typeof(o) { "@babel/helpers - typeof"; return _typeof = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function (o) { return typeof o; } : function (o) { return o && "function" == typeof Symbol && o.constructor === Symbol && o !== Symbol.prototype ? "symbol" : typeof o; }, _typeof(o); }
function _defineProperty(e, r, t) { return (r = _toPropertyKey(r)) in e ? Object.defineProperty(e, r, { value: t, enumerable: !0, configurable: !0, writable: !0 }) : e[r] = t, e; }
function _toPropertyKey(t) { var i = _toPrimitive(t, "string"); return "symbol" == _typeof(i) ? i : i + ""; }
function _toPrimitive(t, r) { if ("object" != _typeof(t) || !t) return t; var e = t[Symbol.toPrimitive]; if (void 0 !== e) { var i = e.call(t, r || "default"); if ("object" != _typeof(i)) return i; throw new TypeError("@@toPrimitive must return a primitive value."); } return ("string" === r ? String : Number)(t); }
/**
 * @return ES6 `for ... of` iterator.
 */
function searchNumbers() {
  var _normalizeArguments = (0, _normalizeArguments2["default"])(arguments),
    text = _normalizeArguments.text,
    options = _normalizeArguments.options,
    metadata = _normalizeArguments.metadata;
  var matcher = new _PhoneNumberMatcher["default"](text, options, metadata);
  return _defineProperty({}, Symbol.iterator, function () {
    return {
      next: function next() {
        if (matcher.hasNext()) {
          return {
            done: false,
            value: matcher.next()
          };
        }
        return {
          done: true
        };
      }
    };
  });
}
//# sourceMappingURL=searchNumbers.js.map