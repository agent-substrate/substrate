function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
import normalizeArguments from './normalizeArguments.js';
import parsePhoneNumberWithError from './parsePhoneNumberWithError_.js';
import ParseError from './ParseError.js';
import Metadata from './metadata.js';
import checkNumberLength from './helpers/checkNumberLength.js';
export default function validatePhoneNumberLength() {
  let {
    text,
    options,
    metadata
  } = normalizeArguments(arguments);
  options = _extends({}, options, {
    extract: false
  });

  // Parse phone number.
  try {
    const phoneNumber = parsePhoneNumberWithError(text, options, metadata);
    const metadataInstance = new Metadata(metadata);
    metadataInstance.selectNumberingPlan(phoneNumber.country || phoneNumber.countryCallingCode);
    const result = checkNumberLength(phoneNumber.nationalNumber, undefined, metadataInstance);
    if (result !== 'IS_POSSIBLE') {
      return result;
    }
  } catch (error) {
    /* istanbul ignore else */
    if (error instanceof ParseError) {
      return error.message;
    } else {
      throw error;
    }
  }
}
//# sourceMappingURL=validatePhoneNumberLength.js.map