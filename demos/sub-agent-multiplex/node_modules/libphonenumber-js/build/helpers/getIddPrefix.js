"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = getIddPrefix;
var _metadata = _interopRequireDefault(require("../metadata.js"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
/**
 * Pattern that makes it easy to distinguish whether a region has a single
 * international dialing prefix or not. If a region has a single international
 * prefix (e.g. 011 in USA), it will be represented as a string that contains
 * a sequence of ASCII digits, and possibly a tilde, which signals waiting for
 * the tone. If there are multiple available international prefixes in a
 * region, they will be represented as a regex string that always contains one
 * or more characters that are not ASCII digits or a tilde.
 */
var SINGLE_IDD_PREFIX_REG_EXP = /^[\d]+(?:[~\u2053\u223C\uFF5E][\d]+)?$/;

// If the `country` supports IDD calling, it returns a preferred IDD prefix.
// If the `country` doesn't support IDD calling, it returns `undefined`.
function getIddPrefix(country, callingCode, metadata) {
  var countryMetadata = new _metadata["default"](metadata);
  countryMetadata.selectNumberingPlan(country || callingCode);
  if (countryMetadata.defaultIDDPrefix()) {
    return countryMetadata.defaultIDDPrefix();
  }
  if (SINGLE_IDD_PREFIX_REG_EXP.test(countryMetadata.IDDPrefix())) {
    return countryMetadata.IDDPrefix();
  }
}
//# sourceMappingURL=getIddPrefix.js.map