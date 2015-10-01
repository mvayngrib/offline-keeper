
var path = require('path')
var rimraf = require('rimraf')
var test = require('tape')
var Q = require('q')
var Keeper = require('../')
var testDir = path.resolve('./tmp')

rimraf.sync(testDir)

test('test invalid keys', function (t) {
  t.plan(1)

  var keeper = new Keeper({
    storage: testDir
  })

  keeper.put('a', new Buffer('b'))
    .then(function () {
      t.fail()
    })
    .catch(function (err) {
      t.pass()
    })
    .done()
})

test('put, get', function (t) {
  t.plan(2)

  var keeper = new Keeper({
    storage: testDir
  })

  var k = [
    // infoHashes of values
    '64fe16cc8a0c61c06bc403e02f515ce5614a35f1',
    '0a745fb75a7818acf09f27de1db7a76081d22776',
    'bd45a097c00e557ea871233ea21d27a47f8f21a9'
  ]

  var v = ['1', '2', '3'].map(Buffer)
  keeper.putOne(k[0], v[0])
    .then(function () {
      return keeper.getOne(k[0])
    })
    .then(function (v0) {
      t.deepEqual(v0, v[0])
    })
    .then(function () {
      // keeper should derive key
      return keeper.putMany(v.slice(1))
    })
    .then(function () {
      return keeper.getMany(k)
    })
    .then(function (vals) {
      t.deepEqual(vals, v)
      rimraf.sync(testDir)
    })
    .done()
})
