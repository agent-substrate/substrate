"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = parseNumber;
var _parse = _interopRequireDefault(require("../parse.js"));
var _normalizeArguments2 = _interopRequireDefault(require("../normalizeArguments.js"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
function parseNumber() {
  var _normalizeArguments = (0, _normalizeArguments2["default"])(arguments),
    text = _normalizeArguments.text,
    options = _normalizeArguments.options,
    metadata = _normalizeArguments.metadata;
  return (0, _parse["default"])(text, options, metadata);
}
//# sourceMappingURL=parse.js.map