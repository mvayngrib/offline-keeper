
var typeforce = require('typeforce')
var extend = require('xtend')
var debug = require('debug')('offline-keeper')
var utils = require('@tradle/utils')
var bindAll = require('bindall')
var collectStream = require('stream-collector')
var collect = function (stream) {
  return new Promise((resolve, reject) => {
    collectStream(stream, (err, data) => {
      if (err) reject(err)
      else resolve(data)
    })
  })
}

module.exports = Keeper

function Keeper (options) {
  typeforce({
    db: 'Object'
  }, options)

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
  return ninvoke(this._db, 'get', this._encodeKey(key))
}

Keeper.prototype.getMany = async function (keys) {
  var vals = await allSettled(keys.map(this.getOne))
  return pluckValue(vals)
}

Keeper.prototype._createReadStream = function (opts) {
  opts = opts || {}
  return this._db.createReadStream({
    start: this._prefix,
    end: this._prefix + '\xff'
  , ...opts})
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

Keeper.prototype.getAll = async function () {
  var data = await collect(this._createReadStream())
  var map = {}
  data.forEach(({ key, value }) => map[key] = value)
  return map
}

Keeper.prototype.put = function (arr) {
  if (Array.isArray(arr)) return this.putMany(arr)
  else return this.putOne.apply(this, arguments)
}

Keeper.prototype.putOne = async function (options) {
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

  options = await this._normalizeOptions(options)
  var key = this._encodeKey(options.key)
  var val = options.value
  await this._validate(key, val)
  return await this._doPut(key, val)
}

Keeper.prototype.putMany = async function (arr) {
  // kick things off
  var promises = arr.map(this.putOne, this)
  var maybes = await allSettled(promises)
  return pluckValue(maybes)
}

Keeper.prototype._validate = async function (key, value) {
  try {
    typeforce('String', key)
    typeforce('Buffer', value)
  } catch (err) {
    debug('invalid options')
    throw err
  }

  var infoHash = await getInfoHash(value)
  if (key !== infoHash) {
    debug('invalid key')
    throw new Error('Key must be the infohash of the value, in this case: ' + infoHash)
  }
}

Keeper.prototype._normalizeOptions = async function (options) {
  var key = options.key
  if (key == null) {
    key = await getInfoHash(options.value)
  }

  return { ...options, key }
}

Keeper.prototype._doPut = function (key, value) {
  if (this._closed) return Promise.reject(new Error('Keeper is closed'))

  if (this._done[key]) return Promise.resolve(key)
  if (this._pending[key]) return this._pending[key]

  return this._pending[key] = this._exists(key)
    .then(exists => {
      if (!exists) return this._save(key, value)
    })
    .then(() => {
      delete this._pending[key]
      this._done[key] = true
      debug('put finished', key)
      return key
    })
}

Keeper.prototype.removeOne = function (key) {
  return ninvoke(this._db, 'del', this._encodeKey(key))
}

Keeper.prototype.removeMany = function (keys) {
  var batch = keys.map((key) => {
    return {
      type: 'del',
      key: this._encodeKey(key)
    }
  }, this)

  return ninvoke(this._db, 'batch', batch)
}

Keeper.prototype.clear = async function () {
  var keys = await this.getAllKeys()
  return await this.removeMany(keys)
}

Keeper.prototype.destroy =
Keeper.prototype.close = async function () {
  this._closed = true
  await allSettled(getValues(this._pending))
  // return native promise
  return await ninvoke(this._db, 'close')
}

Keeper.prototype._exists = async function (key) {
  try {
    await this._getOne(key)
    return true
  } catch (err) {
    return false
  }
}

Keeper.prototype._save = function (key, val) {
  return ninvoke(this._db, 'put', key, val)
}

function getInfoHash (val) {
  return ninvoke(utils, 'getInfoHash', val)
}

function pluckValue (results) {
  return results.map((r) => r.value)
}

function getValues (obj) {
  var v = []
  for (var p in obj) v.push(obj[p])
  return v
}

function ninvoke(obj, method, ...args) {
  return new Promise((resolve, reject) => {
    var callback = (err, ...results) => {
      if (err) return reject(err)

      if (results.length === 1) {
        resolve(results[0])
      } else {
        resolve(results)
      }
    }

    args.push(callback)
    return obj[method].apply(obj, args)
  })
}

async function allSettled (promises) {
  var vals = []
  var i = promises.length
  while (i--) {
    try {
      var value = await promises[i]
      vals.unshift({ value })
    } catch (err) {
      vals.unshift({ reason: err })
    }
  }

  return vals
}

// function denodeify (obj, method, ...args) {
//   return () => {
//     return new Promise((resolve, reject) => {
//       fn.
//     })
//   }
// }
