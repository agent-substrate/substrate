"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.DIGIT_PLACEHOLDER = void 0;
exports.closeNonPairedParens = closeNonPairedParens;
exports.countOccurences = countOccurences;
exports.cutAndStripNonPairedParens = cutAndStripNonPairedParens;
exports.populateTemplateWithDigits = populateTemplateWithDigits;
exports.repeat = repeat;
exports.stripNonPairedParens = stripNonPairedParens;
function _createForOfIteratorHelperLoose(r, e) { var t = "undefined" != typeof Symbol && r[Symbol.iterator] || r["@@iterator"]; if (t) return (t = t.call(r)).next.bind(t); if (Array.isArray(r) || (t = _unsupportedIterableToArray(r)) || e && r && "number" == typeof r.length) { t && (r = t); var o = 0; return function () { return o >= r.length ? { done: !0 } : { done: !1, value: r[o++] }; }; } throw new TypeError("Invalid attempt to iterate non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }
function _unsupportedIterableToArray(r, a) { if (r) { if ("string" == typeof r) return _arrayLikeToArray(r, a); var t = {}.toString.call(r).slice(8, -1); return "Object" === t && r.constructor && (t = r.constructor.name), "Map" === t || "Set" === t ? Array.from(r) : "Arguments" === t || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(t) ? _arrayLikeToArray(r, a) : void 0; } }
function _arrayLikeToArray(r, a) { (null == a || a > r.length) && (a = r.length); for (var e = 0, n = Array(a); e < a; e++) n[e] = r[e]; return n; }
// Should be the same as `DIGIT_PLACEHOLDER` in `libphonenumber-metadata-generator`.
var DIGIT_PLACEHOLDER = exports.DIGIT_PLACEHOLDER = 'x'; // '\u2008' (punctuation space)
var DIGIT_PLACEHOLDER_MATCHER = new RegExp(DIGIT_PLACEHOLDER);

// Counts all occurences of a symbol in a string.
// Unicode-unsafe (because using `.split()`).
function countOccurences(symbol, string) {
  var count = 0;
  // Using `.split('')` to iterate through a string here
  // to avoid requiring `Symbol.iterator` polyfill.
  // `.split('')` is generally not safe for Unicode,
  // but in this particular case for counting brackets it is safe.
  // for (const character of string)
  for (var _iterator = _createForOfIteratorHelperLoose(string.split('')), _step; !(_step = _iterator()).done;) {
    var character = _step.value;
    if (character === symbol) {
      count++;
    }
  }
  return count;
}

// Repeats a string (or a symbol) N times.
// http://stackoverflow.com/questions/202605/repeat-string-javascript
function repeat(string, times) {
  if (times < 1) {
    return '';
  }
  var result = '';
  while (times > 1) {
    if (times & 1) {
      result += string;
    }
    times >>= 1;
    string += string;
  }
  return result + string;
}
function cutAndStripNonPairedParens(string, cutBeforeIndex) {
  if (string[cutBeforeIndex] === ')') {
    cutBeforeIndex++;
  }
  return stripNonPairedParens(string.slice(0, cutBeforeIndex));
}
function closeNonPairedParens(template, cut_before) {
  var retained_template = template.slice(0, cut_before);
  var opening_braces = countOccurences('(', retained_template);
  var closing_braces = countOccurences(')', retained_template);
  var dangling_braces = opening_braces - closing_braces;
  while (dangling_braces > 0 && cut_before < template.length) {
    if (template[cut_before] === ')') {
      dangling_braces--;
    }
    cut_before++;
  }
  return template.slice(0, cut_before);
}
function stripNonPairedParens(string) {
  var dangling_braces = [];
  var i = 0;
  while (i < string.length) {
    if (string[i] === '(') {
      dangling_braces.push(i);
    } else if (string[i] === ')') {
      dangling_braces.pop();
    }
    i++;
  }
  var start = 0;
  var cleared_string = '';
  dangling_braces.push(string.length);
  for (var _i = 0, _dangling_braces = dangling_braces; _i < _dangling_braces.length; _i++) {
    var index = _dangling_braces[_i];
    cleared_string += string.slice(start, index);
    start = index + 1;
  }
  return cleared_string;
}
function populateTemplateWithDigits(template, position, digits) {
  // Using `.split('')` to iterate through a string here
  // to avoid requiring `Symbol.iterator` polyfill.
  // `.split('')` is generally not safe for Unicode,
  // but in this particular case for `digits` it is safe.
  // for (const digit of digits)
  for (var _iterator2 = _createForOfIteratorHelperLoose(digits.split('')), _step2; !(_step2 = _iterator2()).done;) {
    var digit = _step2.value;
    // If there is room for more digits in current `template`,
    // then set the next digit in the `template`,
    // and return the formatted digits so far.
    // If more digits are entered than the current format could handle.
    if (template.slice(position + 1).search(DIGIT_PLACEHOLDER_MATCHER) < 0) {
      return;
    }
    position = template.search(DIGIT_PLACEHOLDER_MATCHER);
    template = template.replace(DIGIT_PLACEHOLDER_MATCHER, digit);
  }
  return [template, position];
}
//# sourceMappingURL=AsYouTypeFormatter.util.js.map