
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
var debug = require('debug')('fsStorage')

module.exports = Storage

function Storage (options) {
  typeforce({
    path: 'String'
  }, options)

  this._path = options.path
  this._pending = []
  bindAll(this, 'getOne')
}

Storage.prototype.getOne = function (key) {
  return readFile(this._getAbsPathForKey(key))
}

Storage.prototype.getMany = function (keys) {
  var self = this
  return Q.allSettled(keys.map(this.getOne))
    .then(function (results) {
      return results.map(function (r) {
        return r.value
      })
    })
}

Storage.prototype.getAll = function () {
  return getFilesRecursive(this._path)
    .then(function (files) {
      return Q.all(files.map(readFile))
    })
}

Storage.prototype.putOne = function (key, value) {
  var self = this

  if (this._closed) return Q.reject('storage is closed')

  var promise = this._exists(key)
    .then(function (exists) {
      if (exists) {
        throw new Error('value for this key already exists in storage')
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

Storage.prototype.removeOne = function (key) {
  return unlink(this._getAbsPathForKey(key))
}

Storage.prototype.removeMany = function (keys) {
  return unlink(this._getAbsPathForKey(key))
}

Storage.prototype.clear = function () {
  return rimraf(this._path)
}

Storage.prototype.close = function () {
  this._closed = true
  return Q.allSettled([this._pending])
}

Storage.prototype._exists = function (key) {
  var filePath = this._getAbsPathForKey(key)
  return Q.Promise(function (resolve) {
    fs.exists(filePath, resolve)
  })
}

Storage.prototype._getAbsPathForKey = function (key) {
  return path.join(this._path, key.slice(0, 2), key.slice(2))
}

Storage.prototype._save = function (key, val) {
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
