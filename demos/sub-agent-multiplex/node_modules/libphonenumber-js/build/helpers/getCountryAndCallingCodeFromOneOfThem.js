"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = getCountryAndCallingCodeFromOneOfThem;
var _metadata = _interopRequireDefault(require("../metadata.js"));
var _isCountryCode = _interopRequireDefault(require("./isCountryCode.js"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
var USE_NON_GEOGRAPHIC_COUNTRY_CODE = false;
function getCountryAndCallingCodeFromOneOfThem(countryOrCallingCode, metadataJson) {
  var country;
  var callingCode;
  var metadata = new _metadata["default"](metadataJson);
  // If country code is passed then derive `countryCallingCode` from it.
  // Also store the country code as `.country`.
  if ((0, _isCountryCode["default"])(countryOrCallingCode)) {
    country = countryOrCallingCode;
    metadata.selectNumberingPlan(country);
    callingCode = metadata.countryCallingCode();
  } else {
    callingCode = countryOrCallingCode;
    /* istanbul ignore if */
    if (USE_NON_GEOGRAPHIC_COUNTRY_CODE) {
      if (metadata.isNonGeographicCallingCode(callingCode)) {
        country = '001';
      }
    }
  }
  return {
    country: country,
    callingCode: callingCode
  };
}
//# sourceMappingURL=getCountryAndCallingCodeFromOneOfThem.js.map