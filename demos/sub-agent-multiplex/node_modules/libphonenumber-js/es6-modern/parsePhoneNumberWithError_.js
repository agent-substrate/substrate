function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
import parse from './parse.js';
export default function parsePhoneNumberWithError(text, options, metadata) {
  return parse(text, _extends({}, options, {
    v2: true
  }), metadata);
}
//# sourceMappingURL=parsePhoneNumberWithError_.js.map