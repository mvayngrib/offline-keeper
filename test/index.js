
var path = require('path')
var fs = require('fs')
var rimraf = require('rimraf')
var test = require('tape')
var Q = require('q')
var Keeper = require('../')
var testDir = path.resolve('./tmp')
var memdown = require('memdown')
var levelup = require('levelup')
var counter = 0
var newDB = function () {
  return levelup('tmp' + (counter++), { db: memdown, valueEncoding: 'binary' })
}

test('put after delete writes to storage', function (t) {
  var keeper = new Keeper({
    db: newDB()
  })

  var key = '64fe16cc8a0c61c06bc403e02f515ce5614a35f1'
  var val = new Buffer('1')
  var pair = {
    key: key,
    value: val
  }

  keeper.putOne(pair, function (err, result) {
    if (err) throw err

    keeper.removeOne(key, function (err) {
      if (err) throw err

      keeper.putOne(pair, function (err) {
        if (err) throw err

        keeper.getOne(key, function (err, v) {
          if (err) throw err

          t.deepEqual(v, val)
          t.end()
        })
      })
    })
  })
})

test('invalid db encoding', function (t) {
  t.throws(function () {
    var keeper = new Keeper({
      db: levelup('tmp' + (counter++), { db: memdown })
    })
  })

  t.end()
})

test('test invalid keys', function (t) {
  t.plan(1)

  var keeper = new Keeper({
    db: newDB()
  })

  keeper.put({
    key: 'a',
    value: new Buffer('b')
  }, function (err) {
    t.ok(err)
    keeper.close(t.end)
  })
})

test('put same data twice', function (t) {
  t.plan(1)

  var keeper = new Keeper({
    db: newDB()
  })

  put(function (err) {
    if (err) throw err

    put(function (err) {
      if (err) throw err

      t.pass()
    })
  })

  function put (cb) {
    return keeper.put({
      key: '64fe16cc8a0c61c06bc403e02f515ce5614a35f1',
      value: new Buffer('1')
    }, cb)
  }
})

test('put, get', function (t) {
  var keeper = new Keeper({
    db: newDB()
  })

  var k = [
    // infoHashes of values
    '64fe16cc8a0c61c06bc403e02f515ce5614a35f1',
    '0a745fb75a7818acf09f27de1db7a76081d22776',
    'bd45a097c00e557ea871233ea21d27a47f8f21a9'
  ]

  var v = ['1', '2', '3'].map(Buffer)
  keeper.putOne({
    key: k[0],
    value: v[0]
  }, function (err) {
    if (err) throw err

    keeper.getOne(k[0], function (err, v0) {
      if (err) throw err

      t.deepEqual(v0, v[0])
      // keeper should derive key
      return keeper.putMany(v.slice(1).map(function (v) {
        return { value: v }
      }), function (err) {
        if (err) throw err

        return keeper.getMany(k, function (err, vals) {
          if (err) throw err

          t.deepEqual(vals, v)
          keeper.getAllKeys(function (err, keys) {
            if (err) throw err

            t.deepEqual(keys.sort(), k.slice().sort())
            keeper.getAllValues(function (err, vals) {
              if (err) throw err

              t.deepEqual(vals.sort(), v.slice().sort())
              keeper.getAll(function (err, map) {
                if (err) throw err

                t.deepEqual(Object.keys(map).sort(), k.slice().sort())
                var vals = Object.keys(map).map(function (key) {
                  return map[key]
                })

                t.deepEqual(vals.sort(), v.slice().sort())
                rimraf.sync(testDir)
                t.end()
              })
            })
          })
        })
      })
    })
  })
})
