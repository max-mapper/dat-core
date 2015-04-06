var dat = require('../')
var tape = require('tape')
var memdb = require('memdb')

var create = function () {
  return dat(memdb(), {valueEncoding: 'utf-8'})
}

tape('clone', function (t) {
  var db = create()

  db.put('hello', 'world', function () {
    db.put('hello', 'welt', function () {
      db.put('test', 'test', function () {
        var clone = create()
        var rs = clone.replicate()

        rs.pipe(db.replicate()).pipe(rs).on('end', function () {
          clone.flush(function () { // TODO: remove flush
            clone.heads(function (err, heads) {
              t.error(err, 'no err')
              clone = clone.checkout(heads[0])
              clone.get('test', function (err, val) {
                t.error(err, 'no err')
                t.same(val, 'test')
                clone.get('hello', function (err, val) {
                  t.error(err, 'no err')
                  t.same(val, 'welt')
                  t.end()
                })
              })
            })
          })
        })
      })
    })
  })
})
