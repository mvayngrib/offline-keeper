
var path = require('path')
var fs = require('fs')
var test = require('tape')
var memdown = require('memdown')
var levelup = require('levelup')
var Q = require('q')
var Keeper = require('../')
var counter = 0

function newDB () {
  return levelup('blah' + (counter++), { db: memdown })
}

test('test invalid keys', async function (t) {
  t.plan(1)

  var keeper = new Keeper({
    db: newDB()
  })

  try {
    await keeper.put('a', new Buffer('b'))
    t.fail('invalid put succeeded')
  } catch (err) {
    t.pass()
  }

  await keeper.close()
  t.end()
})

test('put same data twice', async function (t) {
  var keeper = new Keeper({
    db: newDB()
  })

  try {
    await put()
    await put()
    await keeper.close()
  } catch (err) {
    t.fail(err)
  }

  t.end()

  function put () {
    return keeper.put('64fe16cc8a0c61c06bc403e02f515ce5614a35f1', new Buffer('1'))
  }
})

test('put, get', async function (t) {
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
  await keeper.putOne(k[0], v[0])
  var v0 = await keeper.getOne(k[0])
  t.deepEqual(v0, v[0])
  // keeper should derive key
  await keeper.putMany(v.slice(1))
  var vals = await keeper.getMany(k)
  t.deepEqual(vals, v)
  var keys = await keeper.getAllKeys()
  t.deepEqual(keys.sort(), k.slice().sort())
  vals = await keeper.getAllValues()
  t.deepEqual(vals.sort(), v.slice().sort())
  var map = await keeper.getAll()
  t.deepEqual(Object.keys(map).sort(), k.slice().sort())
  vals = Object.keys(map).map(function (key) {
    return map[key]
  })

  t.deepEqual(vals.sort(), v.slice().sort())
  await keeper.close()
  t.end()
})
