
var fs = require('fs')
var typeforce = require('typeforce')
var Q = require('q')
var fs = require('fs')
var walk = require('walk')
var rimraf = Q.nfbind(require('rimraf'))
var mkdirp = require('mkdirp')
var path = require('path')
var readFile = Q.nfbind(fs.readFile)
var writeFile = Q.nfbind(fs.writeFile)
var unlink = Q.nfbind(fs.unlink)
var debug = require('debug')('offline-keeper')
var utils = require('tradle-utils')
var bindAll = require('bindall')

module.exports = Keeper

function Keeper (options) {
  typeforce({
    flat: '?Boolean',
    storage: 'String'
  }, options)

  this._flat = options.flat
  this._path = options.storage
  this._pending = []
  bindAll(this, 'getOne', 'getMany')
}

Keeper.prototype.get = function (keys) {
  if (Array.isArray(keys)) return this.getMany(keys)
  else return this.getOne(keys)
}

Keeper.prototype.getOne = function (key) {
  return readFile(this._getAbsPathForKey(key))
}

Keeper.prototype.getMany = function (keys) {
  return Q.allSettled(keys.map(this.getOne, this))
    .then(pluckValue)
}

Keeper.prototype.getAll = function () {
  return getFilesRecursive(this._path)
    .then(function (files) {
      return Q.all(files.map(readFile))
    })
}

Keeper.prototype.getAllKeys = function () {
  var self = this
  return getFilesRecursive(this._path)
    .then(function (files) {
      return files.map(self._getKeyForPath, self)
    })
}

Keeper.prototype.getAllValues = function (files) {
  return this.getAllKeys()
    .then(this.getMany)
}

Keeper.prototype.getAll = function (files) {
  var self = this
  var keys
  return this.getAllKeys()
    .then(function (_keys) {
      keys = _keys
      return self.getMany(keys)
    })
    .then(function (values) {
      var map = {}
      for (var i = 0; i < keys.length; i++) {
        map[keys[i]] = values[i]
      }

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
      key = norm.key
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

  if (this._closed) return Q.reject('Keeper is closed')

  var promise = this._exists(key)
    .then(function (exists) {
      if (exists) {
        debug('put aborted, value exists', key)
        return true
        // throw new Error('value for this key already exists in Keeper')
      }

      return self._save(key, value)
    })
    .finally(function () {
      self._pending.splice(self._pending.indexOf(promise), 1)
      debug('put finished', key)
      return key
    })

  this._pending.push(promise)
  return promise
}

Keeper.prototype.removeOne = function (key) {
  return unlink(this._getAbsPathForKey(key))
}

Keeper.prototype.removeMany = function (keys) {
  return unlink(this._getAbsPathForKey(key))
}

Keeper.prototype.clear = function () {
  return rimraf(this._path)
}

Keeper.prototype.close = function () {
  this._closed = true
  return Q.allSettled([this._pending])
}

Keeper.prototype.destroy =
Keeper.prototype.close = function () {
  return Q.resolve()
}

Keeper.prototype._exists = function (key) {
  var filePath = this._getAbsPathForKey(key)
  return Q.Promise(function (resolve) {
    fs.exists(filePath, resolve)
  })
}

Keeper.prototype._getAbsPathForKey = function (key) {
  if (this._flat) {
    return path.join(this._path, key)
  } else {
    // git style
    return path.join(this._path, key.slice(0, 2), key.slice(2))
  }
}

Keeper.prototype._getKeyForPath = function (filePath) {
  var parts = filePath.split('/')
  var file = parts.pop()
  if (this._flat) return file

  return parts.pop() + file
}

Keeper.prototype._save = function (key, val) {
  var filePath = this._getAbsPathForKey(key)
  var dir = path.dirname(filePath)
  return Q.nfcall(mkdirp, dir)
    .then(function () {
      return writeFile(filePath, val)
    })
}

function getFilesRecursive (dir) {
  var deferred = Q.defer()
  var files = []
  // Walker options
  var walker = walk.walk(dir, {
    followLinks: false
  })

  walker.on('file', function (root, stat, next) {
    // Add this file to the list of files
    files.push(root + '/' + stat.name)
    next()
  })

  walker.on('errors', function (root, nodeStatsArray, next) {
    debug('failed to read file', nodeStatsArray)
    next()
  })

  walker.on('end', function () {
    deferred.resolve(files)
  })

  return deferred.promise
}

function getInfoHash (val) {
  return Q.ninvoke(utils, 'getInfoHash', val)
}

function pluckValue (results) {
  return results.map(function (r) {
    return r.value
  })
}
