"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = isCountryCode;
var COUNTRY_CODE_REG_EXP = /^[A-Z]{2}$/;
function isCountryCode(string) {
  return COUNTRY_CODE_REG_EXP.test(string);
}
//# sourceMappingURL=isCountryCode.js.map