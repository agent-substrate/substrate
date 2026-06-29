"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = isCallingCode;
var CALLING_CODE_REG_EXP = /^\d+$/;
function isCallingCode(string) {
  return CALLING_CODE_REG_EXP.test(string);
}
//# sourceMappingURL=isCallingCode.js.map