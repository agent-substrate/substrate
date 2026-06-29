function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
import PhoneNumberMatcher from './PhoneNumberMatcher.js';
import normalizeArguments from './normalizeArguments.js';
export default function findPhoneNumbersInText() {
  const {
    text,
    options,
    metadata
  } = normalizeArguments(arguments);
  const matcher = new PhoneNumberMatcher(text, _extends({}, options, {
    v2: true
  }), metadata);
  const results = [];
  while (matcher.hasNext()) {
    results.push(matcher.next());
  }
  return results;
}
//# sourceMappingURL=findPhoneNumbersInText.js.map