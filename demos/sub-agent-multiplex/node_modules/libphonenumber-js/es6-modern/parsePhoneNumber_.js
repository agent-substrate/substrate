function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
import parsePhoneNumberWithError from './parsePhoneNumberWithError_.js';
import ParseError from './ParseError.js';
import { isSupportedCountry } from './metadata.js';
export default function parsePhoneNumber(text, options, metadata) {
  // Validate `defaultCountry`.
  if (options && options.defaultCountry && !isSupportedCountry(options.defaultCountry, metadata)) {
    options = _extends({}, options, {
      defaultCountry: undefined
    });
  }
  // Parse phone number.
  try {
    return parsePhoneNumberWithError(text, options, metadata);
  } catch (error) {
    /* istanbul ignore else */
    if (error instanceof ParseError) {
      //
    } else {
      throw error;
    }
  }
}
//# sourceMappingURL=parsePhoneNumber_.js.map