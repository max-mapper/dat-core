var dat = require('../')
var tape = require('tape')
var memdb = require('memdb')

var create = function () {
  return dat(memdb(), {valueEncoding: 'utf-8'})
}

tape('bad checkout', function (t) {
  var db = create()

  db.checkout('not-exist').on('error', function (err) {
    t.ok(err, 'had error')
    t.end()
  })
})

tape('checkout', function (t) {
  var db = create()

  db.put('hello', 'world', function (err) {
    t.error(err, 'no err')
    var hash = db.head
    db.put('hello', 'welt', function (err) {
      t.error(err, 'no err')
      db.checkout(hash).get('hello', function (err, row) {
        t.error(err, 'no err')
        t.same(row.value, 'world')
        t.end()
      })
    })
  })
})

tape('put after checkout', function (t) {
  var db = create()

  db.put('hello', 'world', function (err) {
    t.error(err, 'no err')
    var hash = db.head
    db.put('hello', 'welt', function (err) {
      t.error(err, 'no err')
      var checkout = db.checkout(hash)
      db.put('hello', 'verden', function (err) {
        t.error(err, 'no err')
        db.get('hello', function (err, row) {
          t.error(err, 'no err')
          t.same(row.value, 'verden')
          checkout.get('hello', function (err, row) {
            t.error(err, 'no err')
            t.same(row.value, 'world')
            t.end()
          })
        })
      })
    })
  })
})

tape('put in checkout', function (t) {
  var db = create()

  db.put('hello', 'world', function (err) {
    t.error(err, 'no err')
    var hash = db.head
    db.put('hello', 'welt', function (err) {
      t.error(err, 'no err')
      var checkout = db.checkout(hash)
      checkout.put('hello', 'verden', function (err) {
        t.error(err, 'no err')
        checkout.get('hello', function (err, row) {
          t.error(err, 'no err')
          t.same(row.value, 'verden')
          db.get('hello', function (err, row) {
            t.error(err, 'no err')
            t.same(row.value, 'welt')
            t.end()
          })
        })
      })
    })
  })
})
