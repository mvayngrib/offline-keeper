'use strict';

var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

var typeforce = require('typeforce');
var extend = require('xtend');
var debug = require('debug')('offline-keeper');
var utils = require('@tradle/utils');
var bindAll = require('bindall');
var collectStream = require('stream-collector');
var collect = function collect(stream) {
  return new Promise(function (resolve, reject) {
    collectStream(stream, function (err, data) {
      if (err) reject(err);else resolve(data);
    });
  });
};

module.exports = Keeper;

function Keeper(options) {
  typeforce({
    db: 'Object'
  }, options);

  this._db = options.db;
  this._pending = {};
  this._done = {};
  bindAll(this, 'getOne', 'getMany');
}

Keeper.prototype._encodeKey = function (key) {
  return key;
  // return this._prefix + key
};

Keeper.prototype._decodeKey = function (key) {
  return key;
  // return key.slice(this._prefix.length)
};

Keeper.prototype.get = function (keys) {
  if (Array.isArray(keys)) return this.getMany(keys);else return this.getOne(keys);
};

Keeper.prototype.getOne = function (key) {
  return this._getOne(key);
};

Keeper.prototype._getOne = function (key) {
  return ninvoke(this._db, 'get', this._encodeKey(key));
};

Keeper.prototype.getMany = function _callee(keys) {
  var vals;
  return regeneratorRuntime.async(function _callee$(_context) {
    while (1) switch (_context.prev = _context.next) {
      case 0:
        _context.next = 2;
        return regeneratorRuntime.awrap(allSettled(keys.map(this.getOne)));

      case 2:
        vals = _context.sent;
        return _context.abrupt('return', pluckValue(vals));

      case 4:
      case 'end':
        return _context.stop();
    }
  }, null, this);
};

Keeper.prototype._createReadStream = function (opts) {
  opts = opts || {};
  return this._db.createReadStream(_extends({
    start: this._prefix,
    end: this._prefix + '\xff'
  }, opts));
};

Keeper.prototype.getAllKeys = function () {
  return collect(this._createReadStream({
    values: false
  }));
};

Keeper.prototype.getAllValues = function (files) {
  return collect(this._createReadStream({
    keys: false
  }));
};

Keeper.prototype.getAll = function _callee2() {
  var data, map;
  return regeneratorRuntime.async(function _callee2$(_context2) {
    while (1) switch (_context2.prev = _context2.next) {
      case 0:
        _context2.next = 2;
        return regeneratorRuntime.awrap(collect(this._createReadStream()));

      case 2:
        data = _context2.sent;
        map = {};

        data.forEach(function (_ref) {
          var key = _ref.key;
          var value = _ref.value;
          return map[key] = value;
        });
        return _context2.abrupt('return', map);

      case 6:
      case 'end':
        return _context2.stop();
    }
  }, null, this);
};

Keeper.prototype.put = function (arr) {
  if (Array.isArray(arr)) return this.putMany(arr);else return this.putOne.apply(this, arguments);
};

Keeper.prototype.putOne = function _callee3(options) {
  var key,
      val,
      _args3 = arguments;
  return regeneratorRuntime.async(function _callee3$(_context3) {
    while (1) switch (_context3.prev = _context3.next) {
      case 0:
        if (typeof options === 'string') {
          options = {
            key: _args3[0],
            value: _args3[1]
          };
        } else if (Buffer.isBuffer(options)) {
          options = {
            value: _args3[0]
          };
        }

        _context3.next = 3;
        return regeneratorRuntime.awrap(this._normalizeOptions(options));

      case 3:
        options = _context3.sent;
        key = this._encodeKey(options.key);
        val = options.value;
        _context3.next = 8;
        return regeneratorRuntime.awrap(this._validate(key, val));

      case 8:
        _context3.next = 10;
        return regeneratorRuntime.awrap(this._doPut(key, val));

      case 10:
        return _context3.abrupt('return', _context3.sent);

      case 11:
      case 'end':
        return _context3.stop();
    }
  }, null, this);
};

Keeper.prototype.putMany = function _callee4(arr) {
  var promises, maybes;
  return regeneratorRuntime.async(function _callee4$(_context4) {
    while (1) switch (_context4.prev = _context4.next) {
      case 0:
        // kick things off
        promises = arr.map(this.putOne, this);
        _context4.next = 3;
        return regeneratorRuntime.awrap(allSettled(promises));

      case 3:
        maybes = _context4.sent;
        return _context4.abrupt('return', pluckValue(maybes));

      case 5:
      case 'end':
        return _context4.stop();
    }
  }, null, this);
};

Keeper.prototype._validate = function _callee5(key, value) {
  var infoHash;
  return regeneratorRuntime.async(function _callee5$(_context5) {
    while (1) switch (_context5.prev = _context5.next) {
      case 0:
        _context5.prev = 0;

        typeforce('String', key);
        typeforce('Buffer', value);
        _context5.next = 9;
        break;

      case 5:
        _context5.prev = 5;
        _context5.t0 = _context5['catch'](0);

        debug('invalid options');
        throw _context5.t0;

      case 9:
        _context5.next = 11;
        return regeneratorRuntime.awrap(getInfoHash(value));

      case 11:
        infoHash = _context5.sent;

        if (!(key !== infoHash)) {
          _context5.next = 15;
          break;
        }

        debug('invalid key');
        throw new Error('Key must be the infohash of the value, in this case: ' + infoHash);

      case 15:
      case 'end':
        return _context5.stop();
    }
  }, null, this, [[0, 5]]);
};

Keeper.prototype._normalizeOptions = function _callee6(options) {
  var key;
  return regeneratorRuntime.async(function _callee6$(_context6) {
    while (1) switch (_context6.prev = _context6.next) {
      case 0:
        key = options.key;

        if (!(key == null)) {
          _context6.next = 5;
          break;
        }

        _context6.next = 4;
        return regeneratorRuntime.awrap(getInfoHash(options.value));

      case 4:
        key = _context6.sent;

      case 5:
        return _context6.abrupt('return', _extends({}, options, { key: key }));

      case 6:
      case 'end':
        return _context6.stop();
    }
  }, null, this);
};

Keeper.prototype._doPut = function (key, value) {
  var _this = this;

  if (this._closed) return Promise.reject(new Error('Keeper is closed'));

  if (this._done[key]) return Promise.resolve(key);
  if (this._pending[key]) return this._pending[key];

  return this._pending[key] = this._exists(key).then(function (exists) {
    if (!exists) return _this._save(key, value);
  }).then(function () {
    delete _this._pending[key];
    _this._done[key] = true;
    debug('put finished', key);
    return key;
  });
};

Keeper.prototype.removeOne = function (key) {
  return ninvoke(this._db, 'del', this._encodeKey(key));
};

Keeper.prototype.removeMany = function (keys) {
  var _this2 = this;

  var batch = keys.map(function (key) {
    return {
      type: 'del',
      key: _this2._encodeKey(key)
    };
  }, this);

  return ninvoke(this._db, 'batch', batch);
};

Keeper.prototype.clear = function _callee7() {
  var keys;
  return regeneratorRuntime.async(function _callee7$(_context7) {
    while (1) switch (_context7.prev = _context7.next) {
      case 0:
        _context7.next = 2;
        return regeneratorRuntime.awrap(this.getAllKeys());

      case 2:
        keys = _context7.sent;
        _context7.next = 5;
        return regeneratorRuntime.awrap(this.removeMany(keys));

      case 5:
        return _context7.abrupt('return', _context7.sent);

      case 6:
      case 'end':
        return _context7.stop();
    }
  }, null, this);
};

Keeper.prototype.destroy = Keeper.prototype.close = function _callee8() {
  return regeneratorRuntime.async(function _callee8$(_context8) {
    while (1) switch (_context8.prev = _context8.next) {
      case 0:
        this._closed = true;
        _context8.next = 3;
        return regeneratorRuntime.awrap(allSettled(getValues(this._pending)));

      case 3:
        _context8.next = 5;
        return regeneratorRuntime.awrap(ninvoke(this._db, 'close'));

      case 5:
        return _context8.abrupt('return', _context8.sent);

      case 6:
      case 'end':
        return _context8.stop();
    }
  }, null, this);
};

Keeper.prototype._exists = function _callee9(key) {
  return regeneratorRuntime.async(function _callee9$(_context9) {
    while (1) switch (_context9.prev = _context9.next) {
      case 0:
        _context9.prev = 0;
        _context9.next = 3;
        return regeneratorRuntime.awrap(this._getOne(key));

      case 3:
        return _context9.abrupt('return', true);

      case 6:
        _context9.prev = 6;
        _context9.t0 = _context9['catch'](0);
        return _context9.abrupt('return', false);

      case 9:
      case 'end':
        return _context9.stop();
    }
  }, null, this, [[0, 6]]);
};

Keeper.prototype._save = function (key, val) {
  return ninvoke(this._db, 'put', key, val);
};

function getInfoHash(val) {
  return ninvoke(utils, 'getInfoHash', val);
}

function pluckValue(results) {
  return results.map(function (r) {
    return r.value;
  });
}

function getValues(obj) {
  var v = [];
  for (var p in obj) {
    v.push(obj[p]);
  }return v;
}

function ninvoke(obj, method) {
  for (var _len = arguments.length, args = Array(_len > 2 ? _len - 2 : 0), _key = 2; _key < _len; _key++) {
    args[_key - 2] = arguments[_key];
  }

  return new Promise(function (resolve, reject) {
    var callback = function callback(err) {
      for (var _len2 = arguments.length, results = Array(_len2 > 1 ? _len2 - 1 : 0), _key2 = 1; _key2 < _len2; _key2++) {
        results[_key2 - 1] = arguments[_key2];
      }

      if (err) return reject(err);

      if (results.length === 1) {
        resolve(results[0]);
      } else {
        resolve(results);
      }
    };

    args.push(callback);
    return obj[method].apply(obj, args);
  });
}

function allSettled(promises) {
  var vals, i, value;
  return regeneratorRuntime.async(function allSettled$(_context10) {
    while (1) switch (_context10.prev = _context10.next) {
      case 0:
        vals = [];
        i = promises.length;

      case 2:
        if (! i--) {
          _context10.next = 15;
          break;
        }

        _context10.prev = 3;
        _context10.next = 6;
        return regeneratorRuntime.awrap(promises[i]);

      case 6:
        value = _context10.sent;

        vals.unshift({ value: value });
        _context10.next = 13;
        break;

      case 10:
        _context10.prev = 10;
        _context10.t0 = _context10['catch'](3);

        vals.unshift({ reason: _context10.t0 });

      case 13:
        _context10.next = 2;
        break;

      case 15:
        return _context10.abrupt('return', vals);

      case 16:
      case 'end':
        return _context10.stop();
    }
  }, null, this, [[3, 10]]);
}

// function denodeify (obj, method, ...args) {
//   return () => {
//     return new Promise((resolve, reject) => {
//       fn.
//     })
//   }
// }