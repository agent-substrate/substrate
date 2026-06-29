"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = matchesEntirely;
/**
 * Checks whether the entire input sequence can be matched
 * against the regular expression.
 * @return {boolean}
 */
function matchesEntirely(text, regularExpressionText) {
  // If the assigning of the `''` default value is moved to the arguments above,
  // the code coverage would decrease for some weird reason.
  text = text || '';
  return new RegExp('^(?:' + regularExpressionText + ')$').test(text);
}
//# sourceMappingURL=matchesEntirely.js.map