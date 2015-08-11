var dat = require('../')
var tape = require('tape')
var collect = require('stream-collector')
var memdb = require('memdb')

var create = function () {
  return dat(memdb(), {valueEncoding: 'utf-8'})
}

tape('opens', function (t) {
  var db = create()

  db.on('ready', function () {
    t.ok(true, 'ready')
    t.end()
  })
})

tape('write to dataset', function (t) {
  var db = create()

  var ws = db.createWriteStream({batchSize: 2, dataset: 'test'})

  ws.write({key: 'a', value: 'a'})
  ws.write({key: 'b', value: 'b'})
  ws.write({key: 'c', value: 'c'})

  ws.end(function () {
    collect(db.createKeyStream({dataset: 'no-exist'}), function (err, list) {
      t.error(err, 'no err')
      t.same(list, [])
      collect(db.createKeyStream({dataset: 'test'}), function (err, list) {
        t.error(err, 'no err')
        t.same(list, ['a', 'b', 'c'])
        t.end()
      })
    })
  })
})
