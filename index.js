
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
  return this.putMany([normalizePutArgs.apply(null, arguments)])
}

Keeper.prototype.putMany = function (kvArr) {
  var self = this

  kvArr = kvArr.map(function () {
    return normalizePutArgs.apply(null, arguments)
  })

  kvArr.forEach(function (pair) {
    typeforce({
      key: '?String',
      value: 'Buffer'
    }, pair)
  })

  return Q.all(kvArr.map(this._normalizeOptions))
    .then(function (results) {
      return Q.all(results.map(function (options, i) {
        options.key = self._encodeKey(options.key)
        return self._validate(options.key, options.value)
      }))
    })
    .then(function () {
      return self._doPut(kvArr)
    })
    .then(function () {
      return pluckValue(kvArr)
    })
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

Keeper.prototype._doPut = function (arr) {
  var self = this

  if (this._closed) return Q.reject(new Error('Keeper is closed'))

  var promises = []
  var batch = []
  arr.forEach(function (pair) {
    var cached = self._done[pair.key] || self._pending[pair.key]
    if (cached) {
      promises.push(cached) // cached === true, or a Promise
    } else {
      pair.type = 'put'
      batch.push(pair)
    }
  })

  return Q.all([
      promises,
      this._save(batch)
    ])
    .finally(function () {
      batch.forEach(function (pair) {
        delete self._pending[pair.key]
        self._done[pair.key] = true
      })
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

Keeper.prototype._save = function (batch) {
  return Q.ninvoke(this._db, 'batch', batch)
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

function normalizePutArgs (key, value) {
  if (typeof key === 'string') {
    return {
      key: key,
      value: value
    }
  } else if (Buffer.isBuffer(key)) {
    return {
      value: key
    }
  } else if ('key' in key && 'value' in key) {
    return key
  }

  throw new Error('invalid arguments')
}
