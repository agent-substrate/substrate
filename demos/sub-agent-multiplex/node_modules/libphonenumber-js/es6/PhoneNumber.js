function _typeof(o) { "@babel/helpers - typeof"; return _typeof = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function (o) { return typeof o; } : function (o) { return o && "function" == typeof Symbol && o.constructor === Symbol && o !== Symbol.prototype ? "symbol" : typeof o; }, _typeof(o); }
function ownKeys(e, r) { var t = Object.keys(e); if (Object.getOwnPropertySymbols) { var o = Object.getOwnPropertySymbols(e); r && (o = o.filter(function (r) { return Object.getOwnPropertyDescriptor(e, r).enumerable; })), t.push.apply(t, o); } return t; }
function _objectSpread(e) { for (var r = 1; r < arguments.length; r++) { var t = null != arguments[r] ? arguments[r] : {}; r % 2 ? ownKeys(Object(t), !0).forEach(function (r) { _defineProperty(e, r, t[r]); }) : Object.getOwnPropertyDescriptors ? Object.defineProperties(e, Object.getOwnPropertyDescriptors(t)) : ownKeys(Object(t)).forEach(function (r) { Object.defineProperty(e, r, Object.getOwnPropertyDescriptor(t, r)); }); } return e; }
function _defineProperty(e, r, t) { return (r = _toPropertyKey(r)) in e ? Object.defineProperty(e, r, { value: t, enumerable: !0, configurable: !0, writable: !0 }) : e[r] = t, e; }
function _classCallCheck(a, n) { if (!(a instanceof n)) throw new TypeError("Cannot call a class as a function"); }
function _defineProperties(e, r) { for (var t = 0; t < r.length; t++) { var o = r[t]; o.enumerable = o.enumerable || !1, o.configurable = !0, "value" in o && (o.writable = !0), Object.defineProperty(e, _toPropertyKey(o.key), o); } }
function _createClass(e, r, t) { return r && _defineProperties(e.prototype, r), t && _defineProperties(e, t), Object.defineProperty(e, "prototype", { writable: !1 }), e; }
function _toPropertyKey(t) { var i = _toPrimitive(t, "string"); return "symbol" == _typeof(i) ? i : i + ""; }
function _toPrimitive(t, r) { if ("object" != _typeof(t) || !t) return t; var e = t[Symbol.toPrimitive]; if (void 0 !== e) { var i = e.call(t, r || "default"); if ("object" != _typeof(i)) return i; throw new TypeError("@@toPrimitive must return a primitive value."); } return ("string" === r ? String : Number)(t); }
import Metadata, { validateMetadata } from './metadata.js';
import isPossibleNumber from './isPossible.js';
import isValidNumber from './isValid.js';
import getNumberType from './helpers/getNumberType.js';
import getCountryAndCallingCodeFromOneOfThem from './helpers/getCountryAndCallingCodeFromOneOfThem.js';
import getPossibleCountriesForNumber from './helpers/getPossibleCountriesForNumber.js';
import extractCountryCallingCode from './helpers/extractCountryCallingCode.js';
import isObject from './helpers/isObject.js';
import formatNumber from './format.js';
var USE_NON_GEOGRAPHIC_COUNTRY_CODE = false;
var PhoneNumber = /*#__PURE__*/function () {
  /**
   * @param  {string} countryOrCountryCallingCode
   * @param  {string} nationalNumber
   * @param  {object} metadata — Metadata JSON
   * @return {PhoneNumber}
   */
  function PhoneNumber(countryOrCountryCallingCode, nationalNumber, metadata) {
    _classCallCheck(this, PhoneNumber);
    // Validate `countryOrCountryCallingCode` argument.
    if (!countryOrCountryCallingCode) {
      throw new TypeError('First argument is required');
    }
    if (typeof countryOrCountryCallingCode !== 'string') {
      throw new TypeError('First argument must be a string');
    }

    // In case of public API use: `constructor(number, metadata)`.
    // Transform the arguments from `constructor(number, metadata)` to
    // `constructor(countryOrCountryCallingCode, nationalNumber, metadata)`.
    if (countryOrCountryCallingCode[0] === '+' && !nationalNumber) {
      throw new TypeError('`metadata` argument not passed');
    }
    if (isObject(nationalNumber) && isObject(nationalNumber.countries)) {
      metadata = nationalNumber;
      var e164Number = countryOrCountryCallingCode;
      if (!E164_NUMBER_REGEXP.test(e164Number)) {
        throw new Error('Invalid `number` argument passed: must consist of a "+" followed by digits');
      }
      var _extractCountryCallin = extractCountryCallingCode(e164Number, undefined, undefined, undefined, metadata),
        _countryCallingCode = _extractCountryCallin.countryCallingCode,
        number = _extractCountryCallin.number;
      nationalNumber = number;
      countryOrCountryCallingCode = _countryCallingCode;
      if (!nationalNumber) {
        throw new Error('Invalid `number` argument passed: too short');
      }
    }

    // Validate `nationalNumber` argument.
    if (!nationalNumber) {
      throw new TypeError('`nationalNumber` argument is required');
    }
    if (typeof nationalNumber !== 'string') {
      throw new TypeError('`nationalNumber` argument must be a string');
    }

    // Validate `metadata` argument.
    validateMetadata(metadata);

    // Initialize properties.
    var _getCountryAndCalling = getCountryAndCallingCodeFromOneOfThem(countryOrCountryCallingCode, metadata),
      country = _getCountryAndCalling.country,
      countryCallingCode = _getCountryAndCalling.callingCode;
    this.country = country;
    this.countryCallingCode = countryCallingCode;
    this.nationalNumber = nationalNumber;
    this.number = '+' + this.countryCallingCode + this.nationalNumber;
    // Exclude `metadata` property output from `PhoneNumber.toString()`
    // so that it doesn't clutter the console output of Node.js.
    // Previously, when Node.js did `console.log(new PhoneNumber(...))`,
    // it would output the whole internal structure of the `metadata` object.
    this.getMetadata = function () {
      return metadata;
    };
  }
  return _createClass(PhoneNumber, [{
    key: "setExt",
    value: function setExt(ext) {
      this.ext = ext;
    }
  }, {
    key: "getPossibleCountries",
    value: function getPossibleCountries() {
      if (this.country) {
        return [this.country];
      }
      return getPossibleCountriesForNumber(this.countryCallingCode, this.nationalNumber, this.getMetadata());
    }
  }, {
    key: "isPossible",
    value: function isPossible() {
      return isPossibleNumber(this, {
        v2: true
      }, this.getMetadata());
    }
  }, {
    key: "isValid",
    value: function isValid() {
      return isValidNumber(this, {
        v2: true
      }, this.getMetadata());
    }
  }, {
    key: "isNonGeographic",
    value: function isNonGeographic() {
      var metadata = new Metadata(this.getMetadata());
      return metadata.isNonGeographicCallingCode(this.countryCallingCode);
    }
  }, {
    key: "isEqual",
    value: function isEqual(phoneNumber) {
      return this.number === phoneNumber.number && this.ext === phoneNumber.ext;
    }

    // `validateLength()` method was originally meant to be an equivalent for `validatePhoneNumberLength()`.
    //
    // Later, it became apparent that it's not really a true equivalent.
    // The reason is that a `PhoneNumber` instance is not created
    // when the phone number string is too short for it to be considered a valid phone number:
    // * When there must be at least 2 national (significant) number digits: `"1"`.
    // * When the country calling code part of an international number is incomplete: `"+12"`.
    //
    // So leaving this `validateLength()` method here would suggest a hidden anti-pattern
    // of using it instead of `validatePhoneNumberLength()` while ignoring
    // the "too short to be even possible" case from phone number length validation.
    // And ignoring that case wouldn't make any sense in a real-world application
    // because it would still be a valid case that should be handled.
    //
    // Because of that, this method was eventually commented out in order to not introduce
    // that kind of an anti-pattern.
    //
    // validateLength() {
    // 	const result = checkNumberLength(
    // 		this.nationalNumber,
    // 		undefined,
    // 		this.getMetadata()
    // 	)
    // 	if (result !== 'IS_POSSIBLE') {
    // 		return result
    // 	}
    // }
  }, {
    key: "getType",
    value: function getType() {
      return getNumberType(this, {
        v2: true
      }, this.getMetadata());
    }
  }, {
    key: "format",
    value: function format(_format, options) {
      return formatNumber(this, _format, options ? _objectSpread(_objectSpread({}, options), {}, {
        v2: true
      }) : {
        v2: true
      }, this.getMetadata());
    }
  }, {
    key: "formatNational",
    value: function formatNational(options) {
      return this.format('NATIONAL', options);
    }
  }, {
    key: "formatInternational",
    value: function formatInternational(options) {
      return this.format('INTERNATIONAL', options);
    }
  }, {
    key: "getURI",
    value: function getURI(options) {
      return this.format('RFC3966', options);
    }
  }]);
}();
export { PhoneNumber as default };
var E164_NUMBER_REGEXP = /^\+\d+$/;
//# sourceMappingURL=PhoneNumber.js.map