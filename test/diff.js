var tape = require('tape')
var dat = require('../')

var memdb = require('memdb')

var create = function () {
  return dat(memdb(), {valueEncoding: 'utf-8'})
}

tape('diff', function (t) {
  var db = create()
  var hadData = false

  db.put('hello', 'world', function (err) {
    t.error(err, 'no err')
    var hash = db.head
    db.put('hello', 'welt', function (err) {
      t.error(err, 'no err')
      db.diff(hash, db.head)
        .on('data', function (versions) {
          hadData = true
          t.same(versions.length, 2)
          t.same(versions[0].type, 'put')
          t.same(versions[0].key, 'hello')
          t.same(versions[0].value, 'world')
          t.same(versions[1].type, 'put')
          t.same(versions[1].key, 'hello')
          t.same(versions[1].value, 'welt')
        })
        .on('end', function () {
          t.ok(hadData, 'had data')
          t.end()
        })
    })
  })
})
