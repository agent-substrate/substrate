function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
import normalizeArguments from './normalizeArguments.js';
import parsePhoneNumber from './parsePhoneNumber_.js';
export default function isPossiblePhoneNumber() {
  let {
    text,
    options,
    metadata
  } = normalizeArguments(arguments);
  options = _extends({}, options, {
    extract: false
  });
  const phoneNumber = parsePhoneNumber(text, options, metadata);
  return phoneNumber && phoneNumber.isPossible() || false;
}
//# sourceMappingURL=isPossiblePhoneNumber.js.map