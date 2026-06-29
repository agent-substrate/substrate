"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = getExampleNumber;
var _PhoneNumber = _interopRequireDefault(require("./PhoneNumber.js"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
function getExampleNumber(country, examples, metadata) {
  if (examples[country]) {
    return new _PhoneNumber["default"](country, examples[country], metadata);
  }
}
//# sourceMappingURL=getExampleNumber.js.map