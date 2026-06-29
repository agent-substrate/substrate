"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = void 0;
var _metadata = _interopRequireDefault(require("./metadata.js"));
var _PhoneNumber = _interopRequireDefault(require("./PhoneNumber.js"));
var _checkNumberLength = _interopRequireDefault(require("./helpers/checkNumberLength.js"));
var _AsYouTypeState = _interopRequireDefault(require("./AsYouTypeState.js"));
var _AsYouTypeFormatter = _interopRequireWildcard(require("./AsYouTypeFormatter.js"));
var _AsYouTypeParser = _interopRequireWildcard(require("./AsYouTypeParser.js"));
var _getCountryByCallingCode = _interopRequireDefault(require("./helpers/getCountryByCallingCode.js"));
var _getCountryByNationalNumber = _interopRequireDefault(require("./helpers/getCountryByNationalNumber.js"));
var _isObject = _interopRequireDefault(require("./helpers/isObject.js"));
function _interopRequireWildcard(e, t) { if ("function" == typeof WeakMap) var r = new WeakMap(), n = new WeakMap(); return (_interopRequireWildcard = function _interopRequireWildcard(e, t) { if (!t && e && e.__esModule) return e; var o, i, f = { __proto__: null, "default": e }; if (null === e || "object" != _typeof(e) && "function" != typeof e) return f; if (o = t ? n : r) { if (o.has(e)) return o.get(e); o.set(e, f); } for (var _t in e) "default" !== _t && {}.hasOwnProperty.call(e, _t) && ((i = (o = Object.defineProperty) && Object.getOwnPropertyDescriptor(e, _t)) && (i.get || i.set) ? o(f, _t, i) : f[_t] = e[_t]); return f; })(e, t); }
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
function _typeof(o) { "@babel/helpers - typeof"; return _typeof = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function (o) { return typeof o; } : function (o) { return o && "function" == typeof Symbol && o.constructor === Symbol && o !== Symbol.prototype ? "symbol" : typeof o; }, _typeof(o); }
function _slicedToArray(r, e) { return _arrayWithHoles(r) || _iterableToArrayLimit(r, e) || _unsupportedIterableToArray(r, e) || _nonIterableRest(); }
function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }
function _unsupportedIterableToArray(r, a) { if (r) { if ("string" == typeof r) return _arrayLikeToArray(r, a); var t = {}.toString.call(r).slice(8, -1); return "Object" === t && r.constructor && (t = r.constructor.name), "Map" === t || "Set" === t ? Array.from(r) : "Arguments" === t || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(t) ? _arrayLikeToArray(r, a) : void 0; } }
function _arrayLikeToArray(r, a) { (null == a || a > r.length) && (a = r.length); for (var e = 0, n = Array(a); e < a; e++) n[e] = r[e]; return n; }
function _iterableToArrayLimit(r, l) { var t = null == r ? null : "undefined" != typeof Symbol && r[Symbol.iterator] || r["@@iterator"]; if (null != t) { var e, n, i, u, a = [], f = !0, o = !1; try { if (i = (t = t.call(r)).next, 0 === l) { if (Object(t) !== t) return; f = !1; } else for (; !(f = (e = i.call(t)).done) && (a.push(e.value), a.length !== l); f = !0); } catch (r) { o = !0, n = r; } finally { try { if (!f && null != t["return"] && (u = t["return"](), Object(u) !== u)) return; } finally { if (o) throw n; } } return a; } }
function _arrayWithHoles(r) { if (Array.isArray(r)) return r; }
function _classCallCheck(a, n) { if (!(a instanceof n)) throw new TypeError("Cannot call a class as a function"); }
function _defineProperties(e, r) { for (var t = 0; t < r.length; t++) { var o = r[t]; o.enumerable = o.enumerable || !1, o.configurable = !0, "value" in o && (o.writable = !0), Object.defineProperty(e, _toPropertyKey(o.key), o); } }
function _createClass(e, r, t) { return r && _defineProperties(e.prototype, r), t && _defineProperties(e, t), Object.defineProperty(e, "prototype", { writable: !1 }), e; }
function _toPropertyKey(t) { var i = _toPrimitive(t, "string"); return "symbol" == _typeof(i) ? i : i + ""; }
function _toPrimitive(t, r) { if ("object" != _typeof(t) || !t) return t; var e = t[Symbol.toPrimitive]; if (void 0 !== e) { var i = e.call(t, r || "default"); if ("object" != _typeof(i)) return i; throw new TypeError("@@toPrimitive must return a primitive value."); } return ("string" === r ? String : Number)(t); }
var USE_NON_GEOGRAPHIC_COUNTRY_CODE = false;
var AsYouType = exports["default"] = /*#__PURE__*/function () {
  /**
   * @param {(string|object)?} [optionsOrDefaultCountry] - The default country used for parsing non-international phone numbers. Can also be an `options` object.
   * @param {Object} metadata
   */
  function AsYouType(optionsOrDefaultCountry, metadata) {
    _classCallCheck(this, AsYouType);
    // Metadata instance is created once and stays constant.
    // This is required because other utility classes such as `AsYouTypeFormatter`
    // will use this metadata instance "reference" and will assume that it stays constant.
    this.metadata = new _metadata["default"](metadata);
    var _this$getCountryAndCa = this.getCountryAndCallingCode(optionsOrDefaultCountry),
      _this$getCountryAndCa2 = _slicedToArray(_this$getCountryAndCa, 2),
      defaultCountry = _this$getCountryAndCa2[0],
      defaultCallingCode = _this$getCountryAndCa2[1];
    // `this.defaultCountry` and `this.defaultCallingCode` aren't required to be in sync.
    // For example, `this.defaultCountry` could be `"AR"` and `this.defaultCallingCode` could be `undefined`.
    // So `this.defaultCountry` and `this.defaultCallingCode` are totally independent.
    this.defaultCountry = defaultCountry;
    this.defaultCallingCode = defaultCallingCode;
    this.reset();
  }
  return _createClass(AsYouType, [{
    key: "getCountryAndCallingCode",
    value: function getCountryAndCallingCode(optionsOrDefaultCountry) {
      // Set `defaultCountry` and `defaultCallingCode` options.
      var defaultCountry;
      var defaultCallingCode;
      // Turns out `null` also has type "object". Weird.
      if (optionsOrDefaultCountry) {
        if ((0, _isObject["default"])(optionsOrDefaultCountry)) {
          defaultCountry = optionsOrDefaultCountry.defaultCountry;
          defaultCallingCode = optionsOrDefaultCountry.defaultCallingCode;
        } else {
          defaultCountry = optionsOrDefaultCountry;
        }
      }
      if (defaultCountry && !this.metadata.hasCountry(defaultCountry)) {
        defaultCountry = undefined;
      }
      if (defaultCallingCode) {
        /* istanbul ignore if */
        if (USE_NON_GEOGRAPHIC_COUNTRY_CODE) {
          if (this.metadata.isNonGeographicCallingCode(defaultCallingCode)) {
            defaultCountry = '001';
          }
        }
      }
      return [defaultCountry, defaultCallingCode];
    }

    /**
     * Inputs "next" phone number characters.
     * @param  {string} text
     * @return {string} Formatted phone number characters that have been input so far.
     */
  }, {
    key: "input",
    value: function input(text) {
      var _this$parser$input = this.parser.input(text, this.state),
        digits = _this$parser$input.digits,
        justLeadingPlus = _this$parser$input.justLeadingPlus;
      if (justLeadingPlus) {
        this.formattedOutput = '+';
      } else if (digits) {
        this.determineTheCountryIfNeeded();
        // Match the available formats by the currently available leading digits.
        if (this.state.nationalSignificantNumber) {
          this.formatter.narrowDownMatchingFormats(this.state);
        }
        var formattedNationalNumber;
        if (this.metadata.hasSelectedNumberingPlan()) {
          formattedNationalNumber = this.formatter.format(digits, this.state);
        }
        if (formattedNationalNumber === undefined) {
          // See if another national (significant) number could be re-extracted.
          if (this.parser.reExtractNationalSignificantNumber(this.state)) {
            this.determineTheCountryIfNeeded();
            // If it could, then re-try formatting the new national (significant) number.
            var nationalDigits = this.state.getNationalDigits();
            if (nationalDigits) {
              formattedNationalNumber = this.formatter.format(nationalDigits, this.state);
            }
          }
        }
        this.formattedOutput = formattedNationalNumber ? this.getFullNumber(formattedNationalNumber) : this.getNonFormattedNumber();
      }
      return this.formattedOutput;
    }
  }, {
    key: "reset",
    value: function reset() {
      var _this = this;
      this.state = new _AsYouTypeState["default"]({
        onCountryChange: function onCountryChange(country) {
          // Before version `1.6.0`, the official `AsYouType` formatter API
          // included a `.country` property on an `AsYouType` instance.
          // Since that property (along with the others) have been moved to
          // `this.state`, `this.country` property is emulated for compatibility
          // with the old versions.
          _this.country = country;
        },
        onCallingCodeChange: function onCallingCodeChange(callingCode, country) {
          _this.metadata.selectNumberingPlan(country || callingCode);
          _this.formatter.reset(_this.metadata.numberingPlan, _this.state);
          _this.parser.reset(_this.metadata.numberingPlan);
        }
      });
      this.formatter = new _AsYouTypeFormatter["default"]({
        state: this.state,
        metadata: this.metadata
      });
      this.parser = new _AsYouTypeParser["default"]({
        defaultCountry: this.defaultCountry,
        defaultCallingCode: this.defaultCallingCode,
        metadata: this.metadata,
        state: this.state,
        onNationalSignificantNumberChange: function onNationalSignificantNumberChange() {
          _this.determineTheCountryIfNeeded();
          _this.formatter.reset(_this.metadata.numberingPlan, _this.state);
        }
      });
      this.state.reset({
        country: this.defaultCountry,
        callingCode: this.defaultCallingCode
      });
      this.formattedOutput = '';
      return this;
    }

    /**
     * Returns `true` if the phone number is being input in international format.
     * In other words, returns `true` if and only if the parsed phone number starts with a `"+"`.
     * @return {boolean}
     */
  }, {
    key: "isInternational",
    value: function isInternational() {
      return this.state.international;
    }

    /**
     * Returns the "calling code" part of the phone number when it's being input
     * in an international format.
     * If no valid calling code has been entered so far, returns `undefined`.
     * @return {string} [callingCode]
     */
  }, {
    key: "getCallingCode",
    value: function getCallingCode() {
      // If the number is being input in national format and some "default calling code"
      // has been passed to `AsYouType` constructor, then `this.state.callingCode`
      // is equal to that "default calling code".
      //
      // If the number is being input in national format and no "default calling code"
      // has been passed to `AsYouType` constructor, then returns `undefined`,
      // even if a "default country" has been passed to `AsYouType` constructor.
      //
      if (this.isInternational()) {
        return this.state.callingCode;
      }
    }

    // A legacy alias.
  }, {
    key: "getCountryCallingCode",
    value: function getCountryCallingCode() {
      return this.getCallingCode();
    }

    /**
     * Returns a two-letter country code of the phone number.
     * Returns `undefined` for "non-geographic" phone numbering plans.
     * Returns `undefined` if no phone number has been input yet.
     * @return {string} [country]
     */
  }, {
    key: "getCountry",
    value: function getCountry() {
      var digits = this.state.digits;
      // Return `undefined` if no digits have been input yet.
      if (digits) {
        return this._getCountry();
      }
    }

    /**
     * Returns a two-letter country code of the phone number.
     * Returns `undefined` for "non-geographic" phone numbering plans.
     * @return {string} [country]
     */
  }, {
    key: "_getCountry",
    value: function _getCountry() {
      var country = this.state.country;
      /* istanbul ignore if */
      if (USE_NON_GEOGRAPHIC_COUNTRY_CODE) {
        // `AsYouType.getCountry()` returns `undefined`
        // for "non-geographic" phone numbering plans.
        if (country === '001') {
          return;
        }
      }
      return country;
    }
  }, {
    key: "determineTheCountryIfNeeded",
    value: function determineTheCountryIfNeeded() {
      // Suppose a user enters a phone number in international format,
      // and there're several countries corresponding to that country calling code,
      // and a country has been derived from the number, and then
      // a user enters one more digit and the number is no longer
      // valid for the derived country, so the country should be re-derived
      // on every new digit in those cases.
      //
      // If the phone number is being input in national format,
      // then it could be a case when `defaultCountry` wasn't specified
      // when creating `AsYouType` instance, and just `defaultCallingCode` was specified,
      // and that "calling code" could correspond to a "non-geographic entity",
      // or there could be several countries corresponding to that country calling code.
      // In those cases, `this.country` is `undefined` and should be derived
      // from the number. Again, if country calling code is ambiguous, then
      // `this.country` should be re-derived with each new digit.
      //
      if (!this.state.country || this.isCountryCallingCodeAmbiguous()) {
        this.determineTheCountry();
      }
    }

    // Prepends `+CountryCode ` in case of an international phone number
  }, {
    key: "getFullNumber",
    value: function getFullNumber(formattedNationalNumber) {
      var _this2 = this;
      if (this.isInternational()) {
        var prefix = function prefix(text) {
          return _this2.formatter.getInternationalPrefixBeforeCountryCallingCode(_this2.state, {
            spacing: text ? true : false
          }) + text;
        };
        var callingCode = this.state.callingCode;
        if (!callingCode) {
          return prefix("".concat(this.state.getDigitsWithoutInternationalPrefix()));
        }
        if (!formattedNationalNumber) {
          return prefix(callingCode);
        }
        return prefix("".concat(callingCode, " ").concat(formattedNationalNumber));
      }
      return formattedNationalNumber;
    }
  }, {
    key: "getNonFormattedNationalNumberWithPrefix",
    value: function getNonFormattedNationalNumberWithPrefix() {
      var _this$state = this.state,
        nationalSignificantNumber = _this$state.nationalSignificantNumber,
        prefixBeforeNationalSignificantNumberThatIsNotNationalPrefix = _this$state.prefixBeforeNationalSignificantNumberThatIsNotNationalPrefix,
        nationalPrefix = _this$state.nationalPrefix;
      var number = nationalSignificantNumber;
      var prefix = prefixBeforeNationalSignificantNumberThatIsNotNationalPrefix || nationalPrefix;
      if (prefix) {
        number = prefix + number;
      }
      return number;
    }
  }, {
    key: "getNonFormattedNumber",
    value: function getNonFormattedNumber() {
      var nationalSignificantNumberIsModified = this.state.nationalSignificantNumberIsModified;
      return this.getFullNumber(nationalSignificantNumberIsModified ? this.state.getNationalDigits() : this.getNonFormattedNationalNumberWithPrefix());
    }
  }, {
    key: "getNonFormattedTemplate",
    value: function getNonFormattedTemplate() {
      var number = this.getNonFormattedNumber();
      if (number) {
        return number.replace(/[\+\d]/g, _AsYouTypeFormatter.DIGIT_PLACEHOLDER);
      }
    }
  }, {
    key: "isCountryCallingCodeAmbiguous",
    value: function isCountryCallingCodeAmbiguous() {
      var callingCode = this.state.callingCode;
      var countryCodes = this.metadata.getCountryCodesForCallingCode(callingCode);
      return countryCodes && countryCodes.length > 1;
    }

    // Determines the exact country of the phone number
    // entered so far based on the country phone code
    // and the national phone number.
  }, {
    key: "determineTheCountry",
    value: function determineTheCountry() {
      // Get a more precise country code for this national number.
      // It is possible that the precise country couldn't be determined
      // in cases when there're multiple countries sharing the same calling code.
      // In that case, `country` will be `undefined`.
      var country = (0, _getCountryByCallingCode["default"])(this.isInternational() ? this.state.callingCode : this.defaultCallingCode, {
        nationalNumber: this.state.nationalSignificantNumber,
        metadata: this.metadata
      });
      // If the derived country is indeed more precise,
      // or if no country could be derived for the updated phone number.
      if (country !== this.state.country) {
        // Set (or reset) the more precise country.
        this.state.setCountry(country);
        // If a more precise country could be derived.
        if (country) {
          // Select a more precise telephone numbering plan.
          // For example, consider that `AsYouType` instance was created with `defaultCountry: "US"` argument.
          // In that case, the metadata instance will be using "US" metadata.
          // But then the user could input a Canadian phone number, in which case
          // the metadata instance should be switched to Canadian telephone numbering plan.
          // It is required to do that because later `canFormatCompleteNumber()` function will be called,
          // and that function relies on the precise country being pre-selected in the metadata instance.
          // Or, for example, `checkNumberLength()` function also relies on same thing.
          this.metadata.selectNumberingPlan(country);
        }
      }
    }

    /**
     * Returns a E.164 phone number value for the user's input.
     *
     * For example, for country `"US"` and input `"(222) 333-4444"`
     * it will return `"+12223334444"`.
     *
     * For international phone number input, it will also auto-correct
     * some minor errors such as using a national prefix when writing
     * an international phone number. For example, if the user inputs
     * `"+44 0 7400 000000"` then it will return an auto-corrected
     * `"+447400000000"` phone number value.
     *
     * Will return `undefined` if no digits have been input,
     * or when inputting a phone number in national format and no
     * default country or default "country calling code" have been set.
     *
     * @return {string} [value]
     */
  }, {
    key: "getNumberValue",
    value: function getNumberValue() {
      var _this$state2 = this.state,
        digits = _this$state2.digits,
        callingCode = _this$state2.callingCode,
        country = _this$state2.country,
        nationalSignificantNumber = _this$state2.nationalSignificantNumber;

      // Will return `undefined` if no digits have been input.
      if (!digits) {
        return;
      }
      if (this.isInternational()) {
        if (callingCode) {
          return '+' + callingCode + nationalSignificantNumber;
        } else {
          return '+' + digits;
        }
      } else {
        if (country || callingCode) {
          var callingCode_ = country ? this.metadata.countryCallingCode() : callingCode;
          return '+' + callingCode_ + nationalSignificantNumber;
        }
      }
    }

    /**
     * Returns an instance of `PhoneNumber` class.
     * Will return `undefined` if no national (significant) number
     * digits have been entered so far, or if no `defaultCountry` has been
     * set and the user enters a phone number not in international format.
     */
  }, {
    key: "getNumber",
    value: function getNumber() {
      var _this$state3 = this.state,
        nationalSignificantNumber = _this$state3.nationalSignificantNumber,
        carrierCode = _this$state3.carrierCode,
        callingCode = _this$state3.callingCode;
      if (!nationalSignificantNumber) {
        return;
      }

      // `this._getCountry()` is basically same as `this.state.country`
      // with the only change that it returns `undefined` in case of a
      // "non-geographic" numbering plan instead of `"001"` "internal use" value.
      var country = this._getCountry();

      // Sidenote:
      // `state.callingCode` and `state.country` are somewhat independent from one another
      // and there could be situations when one is defined but the other is not.
      // See the comment in `AsYouTypeState.js` for more info on when that is possible.

      if (!country && !callingCode) {
        return;
      }

      // By default, if `defaultCountry` parameter was passed when
      // creating `AsYouType` instance, `state.country` is gonna be
      // that `defaultCountry`, which doesn't entirely conform with
      // `parsePhoneNumber()`'s behavior where it attempts to determine
      // the country more precisely in cases when multiple countries
      // could correspond to the same `countryCallingCode`.
      // https://gitlab.com/catamphetamine/libphonenumber-js/-/issues/103#note_1417192969
      //
      // Because `AsYouType.getNumber()` method is supposed to be a 1:1
      // equivalent for `parsePhoneNumber(AsYouType.getNumberValue())`,
      // it means that it should also behave accordingly in such cases of `country` ambiguity.
      // That's how users of this library would expect it to behave anyway.
      //
      // So here it checks if `state.country` is same as the `defaultCountry`,
      // and if it is, it attempts to get rid of any potential ambiguity.
      //
      if (country) {
        if (country === this.defaultCountry) {
          // // Create a `Metadata` instance just to get the calling code of the `country`.
          // const metadata = new Metadata(this.metadata.metadata)
          // // `state.callingCode` and `state.country` are somewhat independent from one another
          // // and there could be situations when one is defined but the other is not.
          // // See the comment in `AsYouTypeState.js` for more info on when that is possible.
          // // Here, it only passes `state.country` because there's no need to pass `state.callingCode`
          // // because `state.country` is known to be defined.
          // metadata.selectNumberingPlan(country)
          // const countryCallingCode = metadata.numberingPlan.callingCode()

          // Instead of getting `countryCallingCode` value above,
          // it could simply read it from the existing metadata instance.
          var countryCallingCode = this.metadata.numberingPlan.callingCode();

          // Attempt to rule out any potential country code ambiguity, if present.
          var exactCountry = getExactCountryForMultiCountryCallingCode(countryCallingCode, nationalSignificantNumber, this.metadata);
          if (exactCountry) {
            country = exactCountry;
          }
        }
      }
      var phoneNumber = new _PhoneNumber["default"](country || callingCode, nationalSignificantNumber, this.metadata.metadata);
      if (carrierCode) {
        phoneNumber.carrierCode = carrierCode;
      }
      // Phone number extensions are not supported by "As You Type" formatter.
      return phoneNumber;
    }

    /**
     * Returns `true` if the phone number is "possible".
     * Is just a shortcut for `PhoneNumber.isPossible()`.
     * @return {boolean}
     */
  }, {
    key: "isPossible",
    value: function isPossible() {
      var phoneNumber = this.getNumber();
      if (!phoneNumber) {
        return false;
      }
      return phoneNumber.isPossible();
    }

    /**
     * Returns `true` if the phone number is "valid".
     * Is just a shortcut for `PhoneNumber.isValid()`.
     * @return {boolean}
     */
  }, {
    key: "isValid",
    value: function isValid() {
      var phoneNumber = this.getNumber();
      if (!phoneNumber) {
        return false;
      }
      return phoneNumber.isValid();
    }

    /**
     * Checks if the phone number length is valid.
     * If it is, nothing is returned.
     * Otherwise, a rejection reason is returned.
     * @return {string?}
     */
  }, {
    key: "validateLength",
    value: function validateLength() {
      var _this$state4 = this.state,
        digits = _this$state4.digits,
        nationalSignificantNumber = _this$state4.nationalSignificantNumber;
      if (!digits) {
        return 'NOT_A_NUMBER';
      }
      if (!this.metadata.numberingPlan) {
        return 'INVALID_COUNTRY';
      }
      if (!nationalSignificantNumber) {
        return 'TOO_SHORT';
      }

      // `state.callingCode` and `state.country` are somewhat independent from one another
      // and there could be situations when one is defined but the other is not.
      // See the comment in `AsYouTypeState.js` for more info on when that is possible.
      // Here, it uses both: if `country` is defined then it will use that,
      // otherwise it will use `callingCode`.
      var result = (0, _checkNumberLength["default"])(nationalSignificantNumber, undefined, this.metadata);
      if (result !== 'IS_POSSIBLE') {
        return result;
      }
    }

    /**
     * @deprecated
     * This method is used in `react-phone-number-input/source/input-control.js`
     * in versions before `3.0.16`.
     */
  }, {
    key: "getNationalNumber",
    value: function getNationalNumber() {
      return this.state.nationalSignificantNumber;
    }

    /**
     * Returns the phone number characters entered by the user.
     * @return {string}
     */
  }, {
    key: "getChars",
    value: function getChars() {
      return (this.state.international ? '+' : '') + this.state.digits;
    }

    /**
     * Returns the template for the formatted phone number.
     * @return {string}
     */
  }, {
    key: "getTemplate",
    value: function getTemplate() {
      return this.formatter.getTemplate(this.state) || this.getNonFormattedTemplate() || '';
    }
  }]);
}(); // In situations when multiple countries use the same calling code,
// this function tells what exact country does a given national (significant) number belong to.
function getExactCountryForMultiCountryCallingCode(callingCode, nationalSignificantNumber, metadata) {
  var ambiguousCountries = metadata.getCountryCodesForCallingCode(callingCode);
  if (ambiguousCountries.length > 1) {
    return (0, _getCountryByNationalNumber["default"])(nationalSignificantNumber, ambiguousCountries, metadata.metadata);
  }
}
//# sourceMappingURL=AsYouType.js.map