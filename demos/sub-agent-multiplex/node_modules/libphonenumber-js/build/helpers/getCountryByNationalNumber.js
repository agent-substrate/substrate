"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = getCountryByNationalNumber;
var _metadata = _interopRequireDefault(require("../metadata.js"));
var _getNumberType = _interopRequireDefault(require("./getNumberType.js"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
function _createForOfIteratorHelperLoose(r, e) { var t = "undefined" != typeof Symbol && r[Symbol.iterator] || r["@@iterator"]; if (t) return (t = t.call(r)).next.bind(t); if (Array.isArray(r) || (t = _unsupportedIterableToArray(r)) || e && r && "number" == typeof r.length) { t && (r = t); var o = 0; return function () { return o >= r.length ? { done: !0 } : { done: !1, value: r[o++] }; }; } throw new TypeError("Invalid attempt to iterate non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }
function _unsupportedIterableToArray(r, a) { if (r) { if ("string" == typeof r) return _arrayLikeToArray(r, a); var t = {}.toString.call(r).slice(8, -1); return "Object" === t && r.constructor && (t = r.constructor.name), "Map" === t || "Set" === t ? Array.from(r) : "Arguments" === t || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(t) ? _arrayLikeToArray(r, a) : void 0; } }
function _arrayLikeToArray(r, a) { (null == a || a > r.length) && (a = r.length); for (var e = 0, n = Array(a); e < a; e++) n[e] = r[e]; return n; }
/**
 * Returns the exact country that a given national (significant) number belongs to
 * in case of ambiguity, i.e. when multiple countries share the same "country calling code".
 * @param {string} [nationalNumber]
 * @param {string[]} countries — A list of countries that share the same calling code.
 * @param {object} metadata — Metadata JSON.
 * @returns {string?} Returns the exact country this number belongs to, or `undefined` if the exact country couldn't be determined.
 */
function getCountryByNationalNumber(nationalNumber, countries, metadataJson) {
  // Create a new `metadata` instance because it will be selecting a `country`.
  var metadata = new _metadata["default"](metadataJson);

  // const matchingCountries = []

  for (var _iterator = _createForOfIteratorHelperLoose(countries), _step; !(_step = _iterator()).done;) {
    var country = _step.value;
    metadata.selectNumberingPlan(country);
    // "Leading digits" patterns are only defined for about 20% of all countries.
    // By definition, matching "leading digits" is a sufficient but not a necessary
    // condition for a phone number to belong to a country.
    // The point of "leading digits" check is that it's the fastest one to get a match.
    // https://gitlab.com/catamphetamine/libphonenumber-js/blob/master/METADATA.md#leading_digits
    // I'd suppose that "leading digits" patterns are mutually exclusive for different countries
    // because of the intended use of that feature.
    if (metadata.leadingDigits()) {
      if (nationalNumber && nationalNumber.search(metadata.leadingDigits()) === 0) {
        return country;
      }
    }
    // Else perform full validation with all of those
    // fixed-line/mobile/etc regular expressions.
    else if ((0, _getNumberType["default"])({
      phone: nationalNumber,
      country: country
    }, undefined, metadata.metadata)) {
      // When multiple countries share the same "country calling code",
      // type patterns aren't guaranteed to be unique among them.
      // For example, both `US` and `CA` have the same pattern for `toll_free` numbers.
      // https://gitlab.com/catamphetamine/libphonenumber-js/-/issues/103#note_1417147572
      //
      // That means that this `if` condition could be `true` for multiple countries from the list.
      // Currently, it just returns the first one, which is also the "main" country for the "country calling code".
      // In an example with `toll_free` numbers above, `"US"` would be returned even though
      // it could as well be `"CA"`.
      //
      // There was also a time when this attempted to be overly smart
      // and kept track of all such multiple matching countries
      // and then picked the one that matched the `defaultCountry`, if provided.
      // For example, with `toll_free` numbers above, and with `defaultCountry: "CA"`,
      // it would've returned `"CA"` instead of `"US"`.
      // Later it turned out that such "overly smart" behavior turned out to be just confusing,
      // so this "overly smart" country detection was reverted to returning the "main" country
      // for the "country calling code".
      // https://gitlab.com/catamphetamine/libphonenumber-js/-/issues/154
      //
      return country;
      //
      // The "overly smart" behavior code:
      //
      // if (defaultCountry) {
      // 	if (country === defaultCountry) {
      // 		return country
      // 	} else {
      // 		matchingCountries.push(country)
      // 	}
      // } else {
      // 	return country
      // }
    }
  }

  // // Return the first ("main") one of the `matchingCountries`.
  // if (matchingCountries.length > 0) {
  // 	return matchingCountries[0]
  // }
}
//# sourceMappingURL=getCountryByNationalNumber.js.map