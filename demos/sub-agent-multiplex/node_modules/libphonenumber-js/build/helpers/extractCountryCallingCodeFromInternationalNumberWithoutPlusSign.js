"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = extractCountryCallingCodeFromInternationalNumberWithoutPlusSign;
var _metadata = _interopRequireDefault(require("../metadata.js"));
var _matchesEntirely = _interopRequireDefault(require("./matchesEntirely.js"));
var _extractNationalNumber = _interopRequireDefault(require("./extractNationalNumber.js"));
var _checkNumberLength = _interopRequireDefault(require("./checkNumberLength.js"));
var _getCountryCallingCode = _interopRequireDefault(require("../getCountryCallingCode.js"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
/**
 * Sometimes some people incorrectly input international phone numbers
 * without the leading `+`. This function corrects such input.
 * @param  {string} number — Phone number digits (only digits, no `+`).
 * @param  {string} [country] — Exact country of the phone number.
 * @param  {string} [defaultCountry]
 * @param  {string} [defaultCallingCode]
 * @param  {object} metadataJson
 * @return {object} `{ countryCallingCode: string?, number: string }`, where `countryCallingCode` is the calling code that was extracted from the input `number` string, and `number` is the originally passed `number` without the extracted calling code.
 */
function extractCountryCallingCodeFromInternationalNumberWithoutPlusSign(number, country, defaultCountry, defaultCallingCode, metadataJson) {
  // Validate arguments.
  // The `number` is known to be in a non-international form
  // because there's no leading "+" character.
  // Therefore, there must be either `country` or `defaultCountry` or `defaultCallingCode`.
  // Otherwise, there'd be no source for the calling code to search for in the `number`.
  if (!(country || defaultCountry || defaultCallingCode)) {
    // There's no source for the calling code to search for in the `number`.
    return {
      number: number
    };
  }
  var countryCallingCode = country || defaultCountry ? (0, _getCountryCallingCode["default"])(country || defaultCountry, metadataJson) : defaultCallingCode;
  if (number.indexOf(countryCallingCode) === 0) {
    var metadata = new _metadata["default"](metadataJson);
    metadata.selectNumberingPlan(country || defaultCountry || defaultCallingCode);
    var possibleShorterNumber = number.slice(countryCallingCode.length);
    var _extractNationalNumbe = (0, _extractNationalNumber["default"])(possibleShorterNumber, undefined, metadata),
      possibleShorterNationalNumber = _extractNationalNumbe.nationalNumber;
    var _extractNationalNumbe2 = (0, _extractNationalNumber["default"])(number, undefined, metadata),
      nationalNumber = _extractNationalNumbe2.nationalNumber;

    // If the number was not valid before but is valid now,
    // or if it was too long before, we consider the number
    // with the country calling code stripped to be a better result
    // and keep that instead.
    // For example, in Germany (+49), `49` is a valid area code,
    // so if a number starts with `49`, it could be both a valid
    // national German number or an international number without
    // a leading `+`.
    if (!(0, _matchesEntirely["default"])(nationalNumber, metadata.nationalNumberPattern()) && (0, _matchesEntirely["default"])(possibleShorterNationalNumber, metadata.nationalNumberPattern()) || (0, _checkNumberLength["default"])(nationalNumber, undefined, metadata) === 'TOO_LONG') {
      return {
        countryCallingCode: countryCallingCode,
        number: possibleShorterNumber
      };
    }
  }
  return {
    number: number
  };
}
//# sourceMappingURL=extractCountryCallingCodeFromInternationalNumberWithoutPlusSign.js.map