
var path = require('path')
var rimraf = require('rimraf')
var test = require('tape')
var Q = require('q')
var Keeper = require('../')
var testDir = path.resolve('./tmp')

rimraf.sync(testDir)

test('put, get', function (t) {
  t.plan(2)

  var keeper = new Keeper({
    storage: testDir
  })

  var k = ['a', 'b']
  var v = ['1', '2'].map(Buffer)
  keeper.putOne(k[0], v[0])
    .then(function () {
      return keeper.getOne('a')
    })
    .then(function (v0) {
      t.deepEqual(v0, v[0])
    })
    .then(function () {
      return keeper.putOne(k[1], v[1])
    })
    .then(function () {
      return keeper.getMany(k)
    })
    .then(function (vals) {
      t.deepEqual(vals, v)
    })
    .then(function () {
      return Q.nfapply(rimraf, [testDir])
    })
    .done()
})
