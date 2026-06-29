"use strict";

function _typeof(o) { "@babel/helpers - typeof"; return _typeof = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function (o) { return typeof o; } : function (o) { return o && "function" == typeof Symbol && o.constructor === Symbol && o !== Symbol.prototype ? "symbol" : typeof o; }, _typeof(o); }
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = parse;
var _constants = require("./constants.js");
var _ParseError = _interopRequireDefault(require("./ParseError.js"));
var _metadata = _interopRequireDefault(require("./metadata.js"));
var _isViablePhoneNumber = _interopRequireWildcard(require("./helpers/isViablePhoneNumber.js"));
var _extractExtension = _interopRequireDefault(require("./helpers/extension/extractExtension.js"));
var _parseIncompletePhoneNumber = _interopRequireDefault(require("./parseIncompletePhoneNumber.js"));
var _getCountryCallingCode = _interopRequireDefault(require("./getCountryCallingCode.js"));
var _isPossible = require("./isPossible.js");
var _PhoneNumber = _interopRequireDefault(require("./PhoneNumber.js"));
var _matchesEntirely = _interopRequireDefault(require("./helpers/matchesEntirely.js"));
var _extractCountryCallingCode = _interopRequireDefault(require("./helpers/extractCountryCallingCode.js"));
var _extractNationalNumber = _interopRequireDefault(require("./helpers/extractNationalNumber.js"));
var _getCountryByCallingCode = _interopRequireDefault(require("./helpers/getCountryByCallingCode.js"));
var _extractFormattedPhoneNumberFromPossibleRfc3966NumberUri = _interopRequireDefault(require("./helpers/extractFormattedPhoneNumberFromPossibleRfc3966NumberUri.js"));
function _interopRequireWildcard(e, t) { if ("function" == typeof WeakMap) var r = new WeakMap(), n = new WeakMap(); return (_interopRequireWildcard = function _interopRequireWildcard(e, t) { if (!t && e && e.__esModule) return e; var o, i, f = { __proto__: null, "default": e }; if (null === e || "object" != _typeof(e) && "function" != typeof e) return f; if (o = t ? n : r) { if (o.has(e)) return o.get(e); o.set(e, f); } for (var _t in e) "default" !== _t && {}.hasOwnProperty.call(e, _t) && ((i = (o = Object.defineProperty) && Object.getOwnPropertyDescriptor(e, _t)) && (i.get || i.set) ? o(f, _t, i) : f[_t] = e[_t]); return f; })(e, t); }
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
// This is a port of Google Android `libphonenumber`'s
// `phonenumberutil.js` of December 31th, 2018.
//
// https://github.com/googlei18n/libphonenumber/commits/master/javascript/i18n/phonenumbers/phonenumberutil.js

// import { parseRFC3966 } from './helpers/RFC3966.js'

// import stripIddPrefix from './helpers/stripIddPrefix.js'

// We don't allow input strings for parsing to be longer than 250 chars.
// This prevents malicious input from consuming CPU.
var MAX_INPUT_STRING_LENGTH = 250;

// This consists of the plus symbol, digits, and arabic-indic digits.
var PHONE_NUMBER_START_PATTERN = new RegExp('[' + _constants.PLUS_CHARS + _constants.VALID_DIGITS + ']');

// Regular expression of trailing characters that we want to remove.
// A trailing `#` is sometimes used when writing phone numbers with extensions in US.
// Example: "+1 (645) 123 1234-910#" number has extension "910".
var AFTER_PHONE_NUMBER_END_PATTERN = new RegExp('[^' + _constants.VALID_DIGITS + '#' + ']+$');
var USE_NON_GEOGRAPHIC_COUNTRY_CODE = false;

// Examples:
//
// ```js
// parse('8 (800) 555-35-35', 'RU')
// parse('8 (800) 555-35-35', 'RU', metadata)
// parse('8 (800) 555-35-35', { country: { default: 'RU' } })
// parse('8 (800) 555-35-35', { country: { default: 'RU' } }, metadata)
// parse('+7 800 555 35 35')
// parse('+7 800 555 35 35', metadata)
// ```
//

/**
 * Parses a phone number.
 *
 * parse('123456789', { defaultCountry: 'RU', v2: true }, metadata)
 * parse('123456789', { defaultCountry: 'RU' }, metadata)
 * parse('123456789', undefined, metadata)
 *
 * @param  {string} input
 * @param  {object} [options]
 * @param  {object} metadataJson
 * @return {object|PhoneNumber?} If `options.v2: true` flag is passed, it returns a `PhoneNumber?` instance. Otherwise, returns an object of shape `{ phone: '...', country: '...' }` (or just `{}` if no phone number was parsed).
 */
function parse(text, options, metadataJson) {
  // If assigning the `{}` default value is moved to the arguments above,
  // code coverage would decrease for some weird reason.
  options = options || {};
  var metadata = new _metadata["default"](metadataJson);

  // Validate `defaultCountry`.
  if (options.defaultCountry && !metadata.hasCountry(options.defaultCountry)) {
    if (options.v2) {
      throw new _ParseError["default"]('INVALID_COUNTRY');
    }
    throw new Error("Unknown country: ".concat(options.defaultCountry));
  }

  // Parse the phone number.
  var _parseInput = parseInput(text, options.v2, options.extract),
    formattedPhoneNumber = _parseInput.number,
    ext = _parseInput.ext,
    error = _parseInput.error;

  // If the phone number is not viable then return nothing.
  if (!formattedPhoneNumber) {
    if (options.v2) {
      if (error === 'TOO_SHORT') {
        throw new _ParseError["default"]('TOO_SHORT');
      }
      throw new _ParseError["default"]('NOT_A_NUMBER');
    }
    return {};
  }
  var _parsePhoneNumber = parsePhoneNumber(formattedPhoneNumber, options.defaultCountry, options.defaultCallingCode,
    // If `country` is returned, its numbering plan will also be selected in `metadata`.
    // Otherwise, if `countryCallingCode` is returned, its numbering plan will also be selected in `metadata`.
    // Otherwise, if neither `country` nor `countryCallingCode` are returned, no numbering plan will be selected in `metadata`.
    metadata),
    country = _parsePhoneNumber.country,
    nationalNumber = _parsePhoneNumber.nationalNumber,
    countryCallingCode = _parsePhoneNumber.countryCallingCode,
    countryCallingCodeSource = _parsePhoneNumber.countryCallingCodeSource,
    carrierCode = _parsePhoneNumber.carrierCode;
  if (!metadata.hasSelectedNumberingPlan()) {
    if (options.v2) {
      throw new _ParseError["default"]('INVALID_COUNTRY');
    }
    return {};
  }

  // Validate national (significant) number length.
  if (!nationalNumber || nationalNumber.length < _constants.MIN_LENGTH_FOR_NSN) {
    // Won't throw here because the regexp already demands length > 1.
    /* istanbul ignore if */
    if (options.v2) {
      throw new _ParseError["default"]('TOO_SHORT');
    }
    // Google's demo just throws an error in this case.
    return {};
  }

  // Validate national (significant) number length.
  //
  // A sidenote:
  //
  // They say that sometimes national (significant) numbers
  // can be longer than `MAX_LENGTH_FOR_NSN` (e.g. in Germany).
  // https://github.com/googlei18n/libphonenumber/blob/7e1748645552da39c4e1ba731e47969d97bdb539/resources/phonenumber.proto#L36
  // Such numbers will just be discarded.
  //
  if (nationalNumber.length > _constants.MAX_LENGTH_FOR_NSN) {
    if (options.v2) {
      throw new _ParseError["default"]('TOO_LONG');
    }
    // Google's demo just throws an error in this case.
    return {};
  }
  if (options.v2) {
    var phoneNumber = new _PhoneNumber["default"](countryCallingCode, nationalNumber, metadata.metadata);
    if (country) {
      phoneNumber.country = country;
    }
    if (carrierCode) {
      phoneNumber.carrierCode = carrierCode;
    }
    if (ext) {
      phoneNumber.ext = ext;
    }
    phoneNumber.__countryCallingCodeSource = countryCallingCodeSource;
    return phoneNumber;
  }

  // Check if national phone number pattern matches the number.
  // National number pattern is different for each country,
  // even for those ones which are part of the "NANPA" group.
  var valid = (options.extended ? metadata.hasSelectedNumberingPlan() : country) ? (0, _matchesEntirely["default"])(nationalNumber, metadata.nationalNumberPattern()) : false;
  if (!options.extended) {
    return valid ? result(country, nationalNumber, ext) : {};
  }

  // isInternational: countryCallingCode !== undefined

  return {
    country: country,
    countryCallingCode: countryCallingCode,
    carrierCode: carrierCode,
    valid: valid,
    possible: valid ? true : options.extended === true && metadata.possibleLengths() && (0, _isPossible.isPossibleNumber)(nationalNumber, metadata) ? true : false,
    phone: nationalNumber,
    ext: ext
  };
}

/**
 * Extracts a formatted phone number from text.
 * Doesn't guarantee that the extracted phone number
 * is a valid phone number (for example, doesn't validate its length).
 * @param  {string} text
 * @param  {boolean} [extract] — If `false`, then will parse the entire `text` as a phone number.
 * @param  {boolean} [throwOnError] — By default, it won't throw if the text is too long.
 * @return {string}
 * @example
 * // Returns "(213) 373-4253".
 * extractFormattedPhoneNumber("Call (213) 373-4253 for assistance.")
 */
function _extractFormattedPhoneNumber(text, extract, throwOnError) {
  if (!text) {
    return;
  }
  if (text.length > MAX_INPUT_STRING_LENGTH) {
    if (throwOnError) {
      throw new _ParseError["default"]('TOO_LONG');
    }
    return;
  }
  if (extract === false) {
    return text;
  }
  // Attempt to extract a possible number from the string passed in
  var startsAt = text.search(PHONE_NUMBER_START_PATTERN);
  if (startsAt < 0) {
    return;
  }
  return text
  // Trim everything to the left of the phone number
  .slice(startsAt)
  // Remove trailing non-numerical characters
  .replace(AFTER_PHONE_NUMBER_END_PATTERN, '');
}

/**
 * @param  {string} text - Input.
 * @param  {boolean} v2 - Legacy API functions don't pass `v2: true` flag.
 * @param  {boolean} [extract] - Whether to extract a phone number from `text`, or attempt to parse the entire text as a phone number.
 * @return {object} `{ ?number, ?ext }`.
 */
function parseInput(text, v2, extract) {
  // // Parse RFC 3966 phone number URI.
  // if (text && text.indexOf('tel:') === 0) {
  // 	return parseRFC3966(text)
  // }
  // let number = extractFormattedPhoneNumber(text, extract, v2)
  var number = (0, _extractFormattedPhoneNumberFromPossibleRfc3966NumberUri["default"])(text, {
    extractFormattedPhoneNumber: function extractFormattedPhoneNumber(text) {
      return _extractFormattedPhoneNumber(text, extract, v2);
    }
  });
  // If the phone number is not viable, then abort.
  if (!number) {
    return {};
  }
  if (!(0, _isViablePhoneNumber["default"])(number)) {
    if ((0, _isViablePhoneNumber.isViablePhoneNumberStart)(number)) {
      return {
        error: 'TOO_SHORT'
      };
    }
    return {};
  }
  // Attempt to parse extension first, since it doesn't require region-specific
  // data and we want to have the non-normalised number here.
  var withExtensionStripped = (0, _extractExtension["default"])(number);
  if (withExtensionStripped.ext) {
    return withExtensionStripped;
  }
  return {
    number: number
  };
}

/**
 * Creates `parse()` result object.
 */
function result(country, nationalNumber, ext) {
  var result = {
    country: country,
    phone: nationalNumber
  };
  if (ext) {
    result.ext = ext;
  }
  return result;
}

/**
 * Parses a viable phone number.
 * @param {string} formattedPhoneNumber — Example: "(213) 373-4253".
 * @param {string} [defaultCountry]
 * @param {string} [defaultCallingCode]
 * @param {Metadata} metadata — Metadata instance with no pre-selected numbering plan. If `country` is returned, its numbering plan will also be selected in `metadata`. Otherwise, if `countryCallingCode` is returned, its numbering plan will also be selected in `metadata`. Otherwise, if neither `country` nor `countryCallingCode` are returned, no numbering plan will be selected in `metadata`.
 * @return {object} Returns `{ country: string?, countryCallingCode: string?, nationalNumber: string? }`.
 */
function parsePhoneNumber(formattedPhoneNumber, defaultCountry, defaultCallingCode, metadata) {
  // Attempt to extract a calling code from a phone number.
  // * If the phone number is found to be "international":
  //   * It will attempt to identify the calling code part of it and whether that part is complete and valid.
  //     * If the calling code part is complete and valid, it will return two properties:
  //       that calling code part (without `+`) and the rest of the digits.
  //     * Otherwise, i.e. if the calling code part is incomplete or invalid,
  //       it will return an empty object.
  // * Otherwise, i.e. if the phone number is national, there's no callind code to extract
  //   so it will just return the originally-passed `number` string as the only property.
  var _extractCountryCallin = (0, _extractCountryCallingCode["default"])((0, _parseIncompletePhoneNumber["default"])(formattedPhoneNumber), undefined,
    // `defaultCountry` and `defaultCallingCode` are only used to detect
    // if it's an "international" phone number or not. They won't be used
    // to derive the resulting `countryCallingCode` from them, or anything like that.
    defaultCountry, defaultCallingCode, metadata.metadata),
    countryCallingCodeSource = _extractCountryCallin.countryCallingCodeSource,
    countryCallingCode = _extractCountryCallin.countryCallingCode,
    number = _extractCountryCallin.number;

  // The exact country of the phone number
  var country;

  // If `formattedPhoneNumber` is passed in "international" format,
  // choose a country by `countryCallingCode`.
  if (countryCallingCode) {
    metadata.selectNumberingPlan(countryCallingCode);
  }
  // Else, if `formattedPhoneNumber` is passed in "national" format,
  // then `number` is defined and `countryCallingCode` is `undefined`.
  else if (number && (defaultCountry || defaultCallingCode)) {
    if (defaultCountry) {
      country = defaultCountry;
      metadata.selectNumberingPlan(defaultCountry);
      countryCallingCode = metadata.numberingPlan.callingCode();
    } else {
      metadata.selectNumberingPlan(defaultCallingCode);
      countryCallingCode = defaultCallingCode;
      /* istanbul ignore if */
      if (USE_NON_GEOGRAPHIC_COUNTRY_CODE) {
        if (metadata.isNonGeographicCallingCode(countryCallingCode)) {
          country = '001';
        }
      }
    }
  } else return {};

  // At this point, `country` could be `undefined` but `countryCallingCode` is always defined.
  //
  // Also, if `country` is defined, its numbering plan is selected in `metadata`.
  // Otherwise, the numering plan for the `countryCallingCode` is selected in `metadata`.

  if (!number) {
    return {
      countryCallingCodeSource: countryCallingCodeSource,
      countryCallingCode: countryCallingCode
    };
  }
  var _extractNationalNumbe = (0, _extractNationalNumber["default"])((0, _parseIncompletePhoneNumber["default"])(number), undefined, metadata),
    nationalNumber = _extractNationalNumbe.nationalNumber,
    carrierCode = _extractNationalNumbe.carrierCode;

  // Sometimes there are several countries
  // corresponding to the same country phone code
  // (e.g. NANPA countries all having `1` country phone code).
  // Therefore, to reliably determine the exact country,
  // national (significant) number should have been parsed first.
  //
  // When `metadata.json` is generated, all "ambiguous" country phone codes
  // get their countries populated with the full set of
  // "phone number type" regular expressions.
  //
  var exactCountry = (0, _getCountryByCallingCode["default"])(countryCallingCode, {
    nationalNumber: nationalNumber,
    metadata: metadata
  });
  if (exactCountry) {
    country = exactCountry;
    /* istanbul ignore if */
    if (exactCountry === '001') {
      // Can't happen with `USE_NON_GEOGRAPHIC_COUNTRY_CODE` being `false`.
      // If `USE_NON_GEOGRAPHIC_COUNTRY_CODE` is set to `true` for some reason,
      // then remove the "istanbul ignore if".
    } else {
      metadata.selectNumberingPlan(country);
    }
  }
  return {
    country: country,
    countryCallingCode: countryCallingCode,
    countryCallingCodeSource: countryCallingCodeSource,
    nationalNumber: nationalNumber,
    carrierCode: carrierCode
  };
}
//# sourceMappingURL=parse.js.map