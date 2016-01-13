
var typeforce = require('typeforce')
var Q = require('q')
var extend = require('xtend')
var debug = require('debug')('offline-keeper')
var utils = require('@tradle/utils')
var bindAll = require('bindall')
var collect = require('stream-collector')
collect = Q.nfcall.bind(Q, collect)

module.exports = Keeper

function Keeper (options) {
  typeforce({
    db: 'Object'
  }, options)

  if (options.db.options.valueEncoding !== 'binary') {
    throw new Error('db encoding must be binary')
  }

  this._db = options.db
  this._pending = {}
  this._done = {}
  bindAll(this, 'getOne', 'getMany')
}

Keeper.prototype._encodeKey = function (key) {
  return key
  // return this._prefix + key
}

Keeper.prototype._decodeKey = function (key) {
  return key
  // return key.slice(this._prefix.length)
}

Keeper.prototype.get = function (keys) {
  if (Array.isArray(keys)) return this.getMany(keys)
  else return this.getOne(keys)
}

Keeper.prototype.getOne = function (key) {
  return this._getOne(key)
}

Keeper.prototype._getOne = function (key) {
  return Q.ninvoke(this._db, 'get', this._encodeKey(key))
}

Keeper.prototype.getMany = function (keys) {
  return Q.allSettled(keys.map(this.getOne, this))
    .then(pluckValue)
}

Keeper.prototype._createReadStream = function (opts) {
  return this._db.createReadStream(extend({
    start: this._prefix,
    end: this._prefix + '\xff'
  }, opts || {}))
}

Keeper.prototype.getAll = function () {
  return collect(this._createReadStream())
}

Keeper.prototype.getAllKeys = function () {
  return collect(this._createReadStream({
    values: false
  }))
}

Keeper.prototype.getAllValues = function (files) {
  return collect(this._createReadStream({
    keys: false
  }))
}

Keeper.prototype.getAll = function () {
  return Q.nfcall(collect, this._createReadStream())
    .then(function (data) {
      var map = {}
      data.forEach(function (item) {
        map[item.key] = item.value
      })

      return map
    })
}

Keeper.prototype.put = function (arr) {
  if (Array.isArray(arr)) return this.putMany(arr)
  else return this.putOne.apply(this, arguments)
}

Keeper.prototype.putOne = function (options) {
  var self = this
  if (typeof options === 'string') {
    options = {
      key: arguments[0],
      value: arguments[1]
    }
  } else if (Buffer.isBuffer(options)) {
    options = {
      value: arguments[0]
    }
  }

  var key
  var val
  return this._normalizeOptions(options)
    .then(function (norm) {
      key = self._encodeKey(norm.key)
      val = norm.value
      return self._validate(key, val)
    })
    .then(function () {
      return self._doPut(key, val)
    })
}

Keeper.prototype.putMany = function (arr) {
  return Q.allSettled(arr.map(this.putOne, this))
    .then(pluckValue)
}

Keeper.prototype._validate = function (key, value) {
  try {
    typeforce('String', key)
    typeforce('Buffer', value)
  } catch (err) {
    debug('invalid options')
    return Q.reject(err)
  }

  return getInfoHash(value)
    .then(function (infoHash) {
      if (key !== infoHash) {
        debug('invalid key')
        throw new Error('Key must be the infohash of the value, in this case: ' + infoHash)
      }
    })
}

Keeper.prototype._normalizeOptions = function (options) {
  var getKey = options.key == null ?
    getInfoHash(options.value) :
    Q.resolve(options.key)

  return getKey
    .then(function (key) {
      options.key = key
      return options
    })
}

Keeper.prototype._doPut = function (key, value) {
  var self = this

  if (this._closed) return Q.reject(new Error('Keeper is closed'))

  if (this._done[key]) return Q(key)
  if (this._pending[key]) return this._pending[key]

  return this._pending[key] = this._exists(key)
    .then(function (exists) {
      if (exists) {
        debug('put aborted, value exists', key)
        return true
        // throw new Error('value for this key already exists in Keeper')
      }

      return self._save(key, value)
    })
    .finally(function () {
      delete self._pending[key]
      self._done[key] = true
      debug('put finished', key)
      return key
    })
}

Keeper.prototype._clearCached = function (key) {
  delete this._done[key]
  delete this._pending[key]
}

Keeper.prototype.removeOne = function (key) {
  return this.removeMany([key])
}

Keeper.prototype.removeMany = function (keys) {
  var batch = []
  keys.forEach(function (key) {
    batch.push({
      type: 'del',
      key: this._encodeKey(key)
    })

    this._clearCached(key)
  }, this)

  return Q.ninvoke(this._db, 'batch', batch)
}

Keeper.prototype.clear = function () {
  return this.getAllKeys()
    .then(this.removeMany)
}

Keeper.prototype.destroy =
Keeper.prototype.close = function () {
  var self = this
  this._closed = true
  return Q.allSettled(getValues(this._pending))
    .then(function () {
      return Q.ninvoke(self._db, 'close')
    })
}

Keeper.prototype._exists = function (key) {
  return this._getOne(key)
    .then(function () {
      return true
    })
    .catch(function () {
      return false
    })
}

Keeper.prototype._save = function (key, val) {
  return Q.ninvoke(this._db, 'put', key, val)
}

function getInfoHash (val) {
  return Q.ninvoke(utils, 'getInfoHash', val)
}

function pluckValue (results) {
  return results.map(function (r) {
    return r.value
  })
}

function getValues (obj) {
  var v = []
  for (var p in obj) v.push(obj[p])
  return v
}
