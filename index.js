
var typeforce = require('typeforce')
var extend = require('xtend')
var debug = require('debug')('offline-keeper')
var utils = require('@tradle/utils')
var bindAll = require('bindall')
var collect = require('stream-collector')

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

Keeper.prototype.getOne = function (key, cb) {
  return this._getOne(key, cb)
}

Keeper.prototype._getOne = function (key, cb) {
  return this._db.get(this._encodeKey(key), cb)
}

Keeper.prototype.getMany = function (keys, cb) {
  var self = this
  var togo = keys.length
  var values = []
  keys.forEach(function (key, i) {
    values.push(null)
    self.getOne(key, function (err, value) {
      if (!err) values[i] = value
      if (--togo === 0) {
        cb(null, values)
      }
    })
  })
}

Keeper.prototype._createReadStream = function (opts) {
  return this._db.createReadStream(extend({
    start: this._prefix,
    end: this._prefix + '\xff'
  }, opts || {}))
}

Keeper.prototype.getAll = function (cb) {
  return collect(this._createReadStream(), cb)
}

Keeper.prototype.getAllKeys = function (cb) {
  return collect(this._createReadStream({
    values: false
  }), cb)
}

Keeper.prototype.getAllValues = function (cb) {
  return collect(this._createReadStream({
    keys: false
  }), cb)
}

Keeper.prototype.getAll = function (cb) {
  collect(this._createReadStream(), function (err, data) {
    if (err) return cb(err)

    var map = {}
    data.forEach(function (item) {
      map[item.key] = item.value
    })

    cb(null, map)
  })
}

Keeper.prototype.put = function (arr, cb) {
  return this.putMany([].concat(arr), cb)
}

Keeper.prototype.putOne = function (options, cb) {
  return this.putMany([options], cb)
}

Keeper.prototype.putMany = function (pairs, cb) {
  var self = this
  var error

  var togo = pairs.length
  var normalized = []
  pairs.forEach(function (pair, i) {
    normalized.push(null)
    self._normalizeOptions(pair, function (err, norm) {
      if (failedOut(err)) return

      norm.key = self._encodeKey(norm.key)
      normalized[i] = norm
      self._validate(norm.key, norm.value, function (err) {
        if (failedOut(err)) return
        if (--togo) return

        self._doPut(normalized, function (err) {
          if (err) return cb(err)

          cb(null, pluckValue(normalized))
        })
      })
    })
  })

  function failedOut (err) {
    if (error) return true
    if (err) {
      error = err
      cb(err)
      return true
    }
  }
}

Keeper.prototype._validate = function (key, value, cb) {
  try {
    typeforce('String', key)
    typeforce('Buffer', value)
  } catch (err) {
    debug('invalid options')
    return cb(err)
  }

  return utils.getInfoHash(value, function (err, infoHash) {
    if (err) return cb(err)
    if (key !== infoHash) {
      debug('invalid key')
      return cb(new Error('Key must be the infohash of the value, in this case: ' + infoHash))
    }

    cb()
  })
}

Keeper.prototype._normalizeOptions = function (options, cb) {
  var norm = extend(options)
  getKey(function (err, key) {
    if (err) return cb(err)

    norm.key = key
    cb(null, norm)
  })

  function getKey (cb) {
    if (norm.key) return cb(null, norm.key)

    utils.getInfoHash(norm.value, cb)
  }
}

Keeper.prototype._doPut = function (arr, cb) {
  var self = this

  if (this._closed) return cb(new Error('Keeper is closed'))

  arr.forEach(function (item) {
    item.type = 'put'
  })

  this._db.batch(arr, cb)
}

Keeper.prototype._clearCached = function (key) {
  delete this._done[key]
  delete this._pending[key]
}

Keeper.prototype.removeOne = function (key, cb) {
  return this.removeMany([key], cb)
}

Keeper.prototype.removeMany = function (keys, cb) {
  var batch = []
  keys.forEach(function (key) {
    batch.push({
      type: 'del',
      key: this._encodeKey(key)
    })

    this._clearCached(key)
  }, this)

  this._db.batch(batch, cb)
}

Keeper.prototype.clear = function (cb) {
  var self = this
  this.getAllKeys(function (err, keys) {
    if (err) return cb(err)

    self.removeMany(keys, cb)
  })
}

Keeper.prototype.destroy =
Keeper.prototype.close = function (cb) {
  var self = this
  this._closed = true
  if (!Object.keys(this._pending).length) {
    this._doClose(cb)
  } else {
    this._closeCB = cb
  }
}

Keeper.prototype._doClose = function (cb) {
  this._db.close(cb)
}

Keeper.prototype._exists = function (key, cb) {
  this._getOne(key, function (err) {
    cb(!err)
  })
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

function call (fn) {
  fn()
}
