var dat = require('../')
var tape = require('tape')
var collect = require('stream-collector')
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
            db.heads(function (err, heads) {
              t.error(err, 'no err')
              t.same(heads.length, 2, 'has two heads now')
              t.end()
            })
          })
        })
      })
    })
  })
})

tape('read-stream after checkout', function (t) {
  var db = create()

  db.put('hello', 'world', function () {
    var oldHash = db.head
    db.put('hello', 'verden', function () {
      var oldDb = db.checkout(oldHash)
      oldDb.put('hello', 'mars', function () {
        db.heads(function (_, heads) {
          var rs1 = db.checkout(heads[0]).createReadStream()
          var rs2 = db.checkout(heads[1]).createReadStream()

          collect(rs1, function (_, list1) {
            collect(rs2, function (_, list2) {
              t.same(list1.length, 1)
              t.same(list2.length, 1)
              if (heads[0] === db.head) {
                t.same(list1[0].value, 'verden')
                t.same(list2[0].value, 'mars')
              } else {
                t.same(list1[0].value, 'mars')
                t.same(list2[0].value, 'verden')
              }
              t.end()
            })
          })
        })
      })
    })
  })
})
