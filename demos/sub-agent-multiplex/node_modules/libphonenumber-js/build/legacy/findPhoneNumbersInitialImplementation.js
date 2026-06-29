"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EXTN_PATTERNS_FOR_PARSING = void 0;
exports["default"] = findPhoneNumbers;
exports.searchPhoneNumbers = searchPhoneNumbers;
var _createExtensionPattern = _interopRequireDefault(require("../helpers/extension/createExtensionPattern.js"));
var _PhoneNumberSearch = _interopRequireDefault(require("./PhoneNumberSearch.js"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
// This is a legacy function.
// Use `findNumbers()` instead.

/**
 * Regexp of all possible ways to write extensions, for use when parsing. This
 * will be run as a case-insensitive regexp match. Wide character versions are
 * also provided after each ASCII version. There are three regular expressions
 * here. The first covers RFC 3966 format, where the extension is added using
 * ';ext='. The second more generic one starts with optional white space and
 * ends with an optional full stop (.), followed by zero or more spaces/tabs
 * /commas and then the numbers themselves. The other one covers the special
 * case of American numbers where the extension is written with a hash at the
 * end, such as '- 503#'. Note that the only capturing groups should be around
 * the digits that you want to capture as part of the extension, or else parsing
 * will fail! We allow two options for representing the accented o - the
 * character itself, and one in the unicode decomposed form with the combining
 * acute accent.
 */
var EXTN_PATTERNS_FOR_PARSING = exports.EXTN_PATTERNS_FOR_PARSING = (0, _createExtensionPattern["default"])('parsing');

// // Regular expression for getting opening brackets for a valid number
// // found using `PHONE_NUMBER_START_PATTERN` for prepending those brackets to the number.
// const BEFORE_NUMBER_DIGITS_PUNCTUATION = new RegExp('[' + OPENING_BRACKETS + ']+' + '[' + WHITESPACE + ']*' + '$')

// const VALID_PRECEDING_CHARACTER_PATTERN = /[^a-zA-Z0-9]/

function findPhoneNumbers(text, options, metadata) {
  /* istanbul ignore if */
  if (options === undefined) {
    options = {};
  }
  var search = new _PhoneNumberSearch["default"](text, options, metadata);
  var phones = [];
  while (search.hasNext()) {
    phones.push(search.next());
  }
  return phones;
}

/**
 * @return ES6 `for ... of` iterator.
 */
function searchPhoneNumbers(text, options, metadata) {
  /* istanbul ignore if */
  if (options === undefined) {
    options = {};
  }
  var search = new _PhoneNumberSearch["default"](text, options, metadata);
  var iterator = function iterator() {
    return {
      next: function next() {
        if (search.hasNext()) {
          return {
            done: false,
            value: search.next()
          };
        }
        return {
          done: true
        };
      }
    };
  };

  // This line of code didn't really work with `babel`/`istanbul`:
  // for some weird reason, it showed code coverage less than 100%.
  // That's because `babel`/`istanbul`, for some weird reason,
  // apparently doesn't know how to properly exclude Babel polyfills from code coverage.
  //
  // const iterable = { [Symbol.iterator]: iterator }

  var iterable = {};
  iterable[Symbol.iterator] = iterator;
  return iterable;
}
//# sourceMappingURL=findPhoneNumbersInitialImplementation.js.map