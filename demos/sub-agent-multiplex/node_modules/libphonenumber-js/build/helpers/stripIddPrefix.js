"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = stripIddPrefix;
var _metadata = _interopRequireDefault(require("../metadata.js"));
var _constants = require("../constants.js");
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
var CAPTURING_DIGIT_PATTERN = new RegExp('([' + _constants.VALID_DIGITS + '])');
function stripIddPrefix(number, country, callingCode, metadataJson) {
  if (!country) {
    return;
  }

  // Get the IDD prefix pattern.
  var metadata = new _metadata["default"](metadataJson);
  metadata.selectNumberingPlan(country || callingCode);
  var IDDPrefixPattern = new RegExp(metadata.IDDPrefix());

  // Check if the number is IDD-prefixed.
  if (number.search(IDDPrefixPattern) !== 0) {
    return;
  }

  // Strip IDD prefix.
  number = number.slice(number.match(IDDPrefixPattern)[0].length);

  // If there're any digits after an IDD prefix,
  // then those digits are a country calling code.
  // Since no country code starts with a `0`,
  // the code below validates that the next digit (if present) is not `0`.
  var matchedGroups = number.match(CAPTURING_DIGIT_PATTERN);
  if (matchedGroups && matchedGroups[1] != null && matchedGroups[1].length > 0) {
    if (matchedGroups[1] === '0') {
      return;
    }
  }
  return number;
}
//# sourceMappingURL=stripIddPrefix.js.map