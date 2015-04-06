#!/usr/bin/env node
var dat = require('./')
var http = require('http')
var pump = require('pump')
var url = require('url')
var cmd = process.argv[2]

var db = dat('.', {createIfMissing: cmd === 'init', valueEncoding: 'utf-8'})

var ready = function () {
  if (cmd === 'init') {
    console.log('Created a dat here')
    return
  }

  if (cmd === 'head') {
    console.log(db.head)
    return
  }

  if (cmd === 'checkout') {
    var hash = process.argv[3]
    db.checkout(hash).open(function (err, checkout) {
      if (err) throw err
      db.meta.batch([{type: hash ? 'put' : 'del', key: 'checkout', value: hash}], function () {
        console.log(checkout.head)
      })
    })
    return
  }

  if (cmd === 'log') {
    db.createChangesStream().on('data', function (data) {
      console.log(data.key)
    })
    return
  }

  if (cmd === 'heads') {
    db.heads().on('data', function (head) {
      console.log(head)
    })
    return
  }

  if (cmd === 'layers') {
    db.layers().on('data', function (layer) {
      console.log(layer)
    })
    return
  }

  if (cmd === 'put') {
    db.put(process.argv[3], process.argv[4], function (err) {
      if (err) console.log(err)
      console.log(db.head)
    })
    return
  }

  if (cmd === 'get') {
    db.get(process.argv[3], function (err, value) {
      if (err) console.log(err)
      else console.log(value)
    })
    return
  }

  if (cmd === 'cat') {
    db.createReadStream().on('data', function (data) {
      console.log(data.key + ': ' + data.value)
    })
    return
  }

  if (cmd === 'server') {
    var server = http.createServer(function (req, res) {
      console.log('Replicating with remote peer')
      pump(req, db.createReplicationStream(), res)
    })

    server.listen(6556, function () {
      console.log('dat-core server is listening on port 6556')
    })
    return
  }

  var replicate = function (mode) {
   var u = process.argv[3]
    if (!/:\/\//.test(u)) u = 'http://' + u
    if (!/:\d+/.test(u)) u = u.replace(/\/$/, '') + ':6556'
    u = url.parse(u)

    var req = http.request({method: 'POST', host: u.hostname, port: u.port})
    var rs = db.createReplicationStream({mode: mode})
    var pushed = 0
    var pulled = 0

    rs.on('pull', function () {
      pulled++
    })
    rs.on('push', function () {
      pushed++
    })

    pump(rs, req)
    req.on('error', function () {})
    req.on('response', function (res) {
      pump(res, rs, function () {
        if (mode === 'push') console.log('Pushed %d changes', pushed)
        else if (mode === 'pull') console.log('Pulled %d changes', pulled)
        else console.log('Pulled %d and pushed %d changes', pulled, pushed)
      })
    })
  }

  if (cmd === 'replicate') return replicate()

  if (cmd === 'push') return replicate('push')

  if (cmd === 'pull') return replicate('pull')
}

db.open(function (err) {
  if (err) throw err

  db.meta.get('checkout', function (_, checkout) {
    if (!checkout) return ready()
    db.checkout(checkout).open(function (err, checkout) {
      if (err) throw err
      db = checkout
      ready()
    })
  })
})