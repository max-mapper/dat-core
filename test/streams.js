var dat = require('../')
var tape = require('tape')
var collect = require('stream-collector')
var memdb = require('memdb')

var create = function () {
  return dat(memdb(), {valueEncoding: 'utf-8'})
}

tape('read-stream', function (t) {
  var db = create()

  db.put('hello', 'world', function () {
    collect(db.createReadStream(), function (err, list) {
      t.error(err, 'no err')
      t.same(list.length, 1)
      t.same(list[0].type, 'row')
      t.ok(list[0].version, 'has version')
      t.same(list[0].key, 'hello')
      t.same(list[0].value, 'world')
      t.end()
    })
  })
})

tape('read-stream more than one value', function (t) {
  var db = create()

  db.put('hello', 'world', function () {
    db.put('hej', 'verden', function () {
      collect(db.createReadStream(), function (err, list) {
        t.error(err, 'no err')
        t.same(list.length, 2)
        t.same(list[0].type, 'row')
        t.ok(list[0].version, 'has version')
        t.same(list[0].key, 'hej')
        t.same(list[0].value, 'verden')
        t.same(list[1].type, 'row')
        t.ok(list[1].version, 'has version')
        t.same(list[1].key, 'hello')
        t.same(list[1].value, 'world')
        t.end()
      })
    })
  })
})

tape('read-stream keys are sorted', function (t) {
  var db = create()

  db.put('c', 'c', function () {
    db.put('a', 'a', function () {
      db.put('b', 'b', function () {
        collect(db.createKeyStream(), function (err, list) {
          t.error(err, 'no err')
          t.same(list, ['a', 'b', 'c'])
          t.end()
        })
      })
    })
  })
})

tape('write-stream', function (t) {
  var db = create()

  var ws = db.createWriteStream()

  ws.write({key: 'a', value: 'a'})
  ws.write({key: 'b', value: 'b'})
  ws.write({key: 'c', value: 'c'})

  ws.end(function () {
    collect(db.createKeyStream(), function (err, list) {
      t.error(err, 'no err')
      t.same(list, ['a', 'b', 'c'])
      t.end()
    })
  })
})

tape('write-stream batch', function (t) {
  var db = create()

  var ws = db.createWriteStream()

  ws.write([
    {key: 'a', value: 'a'},
    {key: 'b', value: 'b'},
    {key: 'c', value: 'c'}
  ])

  ws.end(function () {
    collect(db.createKeyStream(), function (err, list) {
      t.error(err, 'no err')
      t.same(list, ['a', 'b', 'c'])
      t.end()
    })
  })
})
