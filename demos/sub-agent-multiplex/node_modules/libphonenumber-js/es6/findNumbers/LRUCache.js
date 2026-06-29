function _typeof(o) { "@babel/helpers - typeof"; return _typeof = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function (o) { return typeof o; } : function (o) { return o && "function" == typeof Symbol && o.constructor === Symbol && o !== Symbol.prototype ? "symbol" : typeof o; }, _typeof(o); }
function _defineProperties(e, r) { for (var t = 0; t < r.length; t++) { var o = r[t]; o.enumerable = o.enumerable || !1, o.configurable = !0, "value" in o && (o.writable = !0), Object.defineProperty(e, _toPropertyKey(o.key), o); } }
function _createClass(e, r, t) { return r && _defineProperties(e.prototype, r), t && _defineProperties(e, t), Object.defineProperty(e, "prototype", { writable: !1 }), e; }
function _toPropertyKey(t) { var i = _toPrimitive(t, "string"); return "symbol" == _typeof(i) ? i : i + ""; }
function _toPrimitive(t, r) { if ("object" != _typeof(t) || !t) return t; var e = t[Symbol.toPrimitive]; if (void 0 !== e) { var i = e.call(t, r || "default"); if ("object" != _typeof(i)) return i; throw new TypeError("@@toPrimitive must return a primitive value."); } return ("string" === r ? String : Number)(t); }
function _classCallCheck(a, n) { if (!(a instanceof n)) throw new TypeError("Cannot call a class as a function"); }
// https://medium.com/dsinjs/implementing-lru-cache-in-javascript-94ba6755cda9
var Node = /*#__PURE__*/_createClass(function Node(key, value) {
  var next = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : null;
  var prev = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : null;
  _classCallCheck(this, Node);
  this.key = key;
  this.value = value;
  this.next = next;
  this.prev = prev;
});
var LRUCache = /*#__PURE__*/function () {
  //set default limit of 10 if limit is not passed.
  function LRUCache() {
    var limit = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : 10;
    _classCallCheck(this, LRUCache);
    this.size = 0;
    this.limit = limit;
    this.head = null;
    this.tail = null;
    this.cache = {};
  }

  // Write Node to head of LinkedList
  // update cache with Node key and Node reference
  return _createClass(LRUCache, [{
    key: "put",
    value: function put(key, value) {
      this.ensureLimit();
      if (!this.head) {
        this.head = this.tail = new Node(key, value);
      } else {
        var node = new Node(key, value, this.head);
        this.head.prev = node;
        this.head = node;
      }

      //Update the cache map
      this.cache[key] = this.head;
      this.size++;
    }

    // Read from cache map and make that node as new Head of LinkedList
  }, {
    key: "get",
    value: function get(key) {
      if (this.cache[key]) {
        var value = this.cache[key].value;

        // node removed from it's position and cache
        this.remove(key);
        // write node again to the head of LinkedList to make it most recently used
        this.put(key, value);
        return value;
      }
      console.log("Item not available in cache for key ".concat(key));
    }
  }, {
    key: "ensureLimit",
    value: function ensureLimit() {
      if (this.size === this.limit) {
        this.remove(this.tail.key);
      }
    }
  }, {
    key: "remove",
    value: function remove(key) {
      var node = this.cache[key];
      if (node.prev !== null) {
        node.prev.next = node.next;
      } else {
        this.head = node.next;
      }
      if (node.next !== null) {
        node.next.prev = node.prev;
      } else {
        this.tail = node.prev;
      }
      delete this.cache[key];
      this.size--;
    }
  }, {
    key: "clear",
    value: function clear() {
      this.head = null;
      this.tail = null;
      this.size = 0;
      this.cache = {};
    }

    // // Invokes the callback function with every node of the chain and the index of the node.
    // forEach(fn) {
    //   let node = this.head;
    //   let counter = 0;
    //   while (node) {
    //     fn(node, counter);
    //     node = node.next;
    //     counter++;
    //   }
    // }

    // // To iterate over LRU with a 'for...of' loop
    // *[Symbol.iterator]() {
    //   let node = this.head;
    //   while (node) {
    //     yield node;
    //     node = node.next;
    //   }
    // }
  }]);
}();
export { LRUCache as default };
//# sourceMappingURL=LRUCache.js.map