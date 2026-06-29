"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.formatRFC3966 = formatRFC3966;
exports.parseRFC3966 = parseRFC3966;
var _isViablePhoneNumber = _interopRequireDefault(require("./isViablePhoneNumber.js"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
function _slicedToArray(r, e) { return _arrayWithHoles(r) || _iterableToArrayLimit(r, e) || _unsupportedIterableToArray(r, e) || _nonIterableRest(); }
function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }
function _iterableToArrayLimit(r, l) { var t = null == r ? null : "undefined" != typeof Symbol && r[Symbol.iterator] || r["@@iterator"]; if (null != t) { var e, n, i, u, a = [], f = !0, o = !1; try { if (i = (t = t.call(r)).next, 0 === l) { if (Object(t) !== t) return; f = !1; } else for (; !(f = (e = i.call(t)).done) && (a.push(e.value), a.length !== l); f = !0); } catch (r) { o = !0, n = r; } finally { try { if (!f && null != t["return"] && (u = t["return"](), Object(u) !== u)) return; } finally { if (o) throw n; } } return a; } }
function _arrayWithHoles(r) { if (Array.isArray(r)) return r; }
function _createForOfIteratorHelperLoose(r, e) { var t = "undefined" != typeof Symbol && r[Symbol.iterator] || r["@@iterator"]; if (t) return (t = t.call(r)).next.bind(t); if (Array.isArray(r) || (t = _unsupportedIterableToArray(r)) || e && r && "number" == typeof r.length) { t && (r = t); var o = 0; return function () { return o >= r.length ? { done: !0 } : { done: !1, value: r[o++] }; }; } throw new TypeError("Invalid attempt to iterate non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }
function _unsupportedIterableToArray(r, a) { if (r) { if ("string" == typeof r) return _arrayLikeToArray(r, a); var t = {}.toString.call(r).slice(8, -1); return "Object" === t && r.constructor && (t = r.constructor.name), "Map" === t || "Set" === t ? Array.from(r) : "Arguments" === t || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(t) ? _arrayLikeToArray(r, a) : void 0; } }
function _arrayLikeToArray(r, a) { (null == a || a > r.length) && (a = r.length); for (var e = 0, n = Array(a); e < a; e++) n[e] = r[e]; return n; }
// https://www.ietf.org/rfc/rfc3966.txt

/**
 * @param  {string} text - Phone URI (RFC 3966).
 * @return {object} `{ ?number, ?ext }`.
 */
function parseRFC3966(text) {
  var number;
  var ext;

  // Replace "tel:" with "tel=" for parsing convenience.
  text = text.replace(/^tel:/, 'tel=');
  for (var _iterator = _createForOfIteratorHelperLoose(text.split(';')), _step; !(_step = _iterator()).done;) {
    var part = _step.value;
    var _part$split = part.split('='),
      _part$split2 = _slicedToArray(_part$split, 2),
      name = _part$split2[0],
      value = _part$split2[1];
    switch (name) {
      case 'tel':
        number = value;
        break;
      case 'ext':
        ext = value;
        break;
      case 'phone-context':
        // Only "country contexts" are supported.
        // "Domain contexts" are ignored.
        if (value[0] === '+') {
          number = value + number;
        }
        break;
    }
  }

  // If the phone number is not viable, then abort.
  if (!(0, _isViablePhoneNumber["default"])(number)) {
    return {};
  }
  var result = {
    number: number
  };
  if (ext) {
    result.ext = ext;
  }
  return result;
}

/**
 * @param  {object} - `{ ?number, ?extension }`.
 * @return {string} Phone URI (RFC 3966).
 */
function formatRFC3966(_ref) {
  var number = _ref.number,
    ext = _ref.ext;
  if (!number) {
    return '';
  }
  if (number[0] !== '+') {
    throw new Error("\"formatRFC3966()\" expects \"number\" to be in E.164 format.");
  }
  return "tel:".concat(number).concat(ext ? ';ext=' + ext : '');
}
//# sourceMappingURL=RFC3966.js.map