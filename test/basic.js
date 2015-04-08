var dat = require('../')
var tape = require('tape')
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

tape('put and get', function (t) {
  var db = create()

  db.put('hello', 'world', function (err) {
    t.error(err, 'no err')
    db.get('hello', function (err, row) {
      t.error(err, 'no err')
      t.same(row.type, 'row')
      t.ok(row.version, 'has version')
      t.same(row.key, 'hello')
      t.same(row.value, 'world')
      t.end()
    })
  })
})

tape('put, put and get', function (t) {
  var db = create()

  db.put('hello', 'world', function (err) {
    t.error(err, 'no err')
    db.put('hello', 'verden', function (err) {
      t.error(err, 'no err')
      db.get('hello', function (err, row) {
        t.error(err, 'no err')
        t.same(row.value, 'verden')
        t.end()
      })
    })
  })
})

tape('put and del', function (t) {
  var db = create()

  db.put('hello', 'world', function (err) {
    t.error(err, 'no err')
    db.del('hello', function (err) {
      t.error(err, 'no err')
      db.get('hello', function (err) {
        t.ok(err, 'had error')
        t.ok(err.notFound, 'not found')
        t.end()
      })
    })
  })
})

tape('batch and get', function (t) {
  var db = create()

  db.batch([{
    type: 'put',
    key: 'hello',
    value: 'world'
  }, {
    type: 'put',
    key: 'hej',
    value: 'verden'
  }], function (err) {
    t.error(err, 'no err')
    db.get('hello', function (err, row) {
      t.error(err, 'no err')
      t.same(row.value, 'world')
      db.get('hej', function (err, row) {
        t.error(err, 'no err')
        t.same(row.value, 'verden')
        t.end()
      })
    })
  })
})
