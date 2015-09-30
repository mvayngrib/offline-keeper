
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
var bindAll = require('bindall')
var debug = require('debug')('fsKeeper')
var utils = require('tradle-utils')

module.exports = Keeper

function Keeper (options) {
  typeforce({
    storage: 'String'
  }, options)

  this._path = options.storage
  this._pending = []
  bindAll(this, 'getOne')
}

Keeper.prototype.getOne = function (key) {
  return readFile(this._getAbsPathForKey(key))
}

Keeper.prototype.getMany = function (keys) {
  var self = this
  return Q.allSettled(keys.map(this.getOne))
    .then(function (results) {
      return results.map(function (r) {
        return r.value
      })
    })
}

Keeper.prototype.getAll = function () {
  return getFilesRecursive(this._path)
    .then(function (files) {
      return Q.all(files.map(readFile))
    })
}

Keeper.prototype.put =
Keeper.prototype.putOne = function (key, value) {
  var self = this
  if (typeof value !== 'undefined') {
    return this._validate(key, value)
      .then(put)
  } else {
    return Q.ninvoke(utils, 'getInfoHash', value)
      .then(function (infoHash) {
        key = infoHash
        return put()
      })
  }

  function put () {
    return self._doPut(key, value)
  }
}

Keeper.prototype._validate = function (key, val) {
  return Q.ninvoke(utils, 'getInfoHash', val)
    .then(function (infoHash) {
      if (key !== infoHash) {
        throw new Error('Key must be the infohash of the value, in this case: ' + infoHash)
      }
    })
}

Keeper.prototype._doPut = function (key, value) {
  var self = this

  if (this._closed) return Q.reject('Keeper is closed')

  var promise = this._exists(key)
    .then(function (exists) {
      if (exists) {
        throw new Error('value for this key already exists in Keeper')
      }

      return self._save(key, value)
    })
    .finally(function (result) {
      self._pending.splice(self._pending.indexOf(promise), 1)
      return result
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

Keeper.prototype._exists = function (key) {
  var filePath = this._getAbsPathForKey(key)
  return Q.Promise(function (resolve) {
    fs.exists(filePath, resolve)
  })
}

Keeper.prototype._getAbsPathForKey = function (key) {
  return path.join(this._path, key.slice(0, 2), key.slice(2))
}

Keeper.prototype._save = function (key, val) {
  var filePath = this._getAbsPathForKey(key)
  var dir = path.dirname(filePath)
  var exists
  return this._exists(key)
    .then(function (_exists) {
      exists = _exists
      return Q.nfcall(mkdirp, dir)
    })
    .then(function () {
      return writeFile(filePath, val)
    })
    .then(function () {
      return !exists
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
