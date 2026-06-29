"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = getCountryByCallingCode;
var _getCountryByNationalNumber = _interopRequireDefault(require("./getCountryByNationalNumber.js"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
var USE_NON_GEOGRAPHIC_COUNTRY_CODE = false;

/**
 * Returns the exact country that a given national (significant) number belongs to
 * in case of ambiguity, i.e. when multiple countries share the same "country calling code".
 * @param {string} [callingCode]
 * @param {string} [options.nationalNumber] — National (significant) number.
 * @param {Metadata} options.metadata — Metadata instance.
 * @returns {string?} Returns the most suitable country for this calling code and national (significant) number.
 */
function getCountryByCallingCode(callingCode, _ref) {
  var nationalNumber = _ref.nationalNumber,
    metadata = _ref.metadata;
  /* istanbul ignore if */
  if (USE_NON_GEOGRAPHIC_COUNTRY_CODE) {
    if (metadata.isNonGeographicCallingCode(callingCode)) {
      return '001';
    }
  }
  var possibleCountries = metadata.getCountryCodesForCallingCode(callingCode);
  if (!possibleCountries) {
    return;
  }
  // If there's just one country corresponding to the country code,
  // then just return it, without further phone number digits validation.
  if (possibleCountries.length === 1) {
    return possibleCountries[0];
  }
  return (0, _getCountryByNationalNumber["default"])(nationalNumber, possibleCountries, metadata.metadata);
}
//# sourceMappingURL=getCountryByCallingCode.js.map