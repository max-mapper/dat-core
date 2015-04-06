#!/usr/bin/env node
var dat = require('./')
var http = require('http')
var url = require('url')
var cmd = process.argv[2]

var db = dat('.', {createIfMissing: cmd === 'init', valueEncoding: 'utf-8'})

if (cmd === 'init') {
  db.open(function () {
    console.log('Created a dat here')
  })
  return
}

if (cmd === 'head') {
  db.open(function () {
    console.log(db.head)
  })
  return
}

if (cmd === 'put') {
  db.put(process.argv[3], process.argv[4], function (err) {
    if (err) console.log(err)
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
}

if (cmd === 'server') {
  var server = http.createServer(function (req, res) {
    req.pipe(db.createReplicationStream()).pipe(res)
  })

  server.listen(6556, function () {
    console.log('dat-core server is listening on port 6556')
  })
  return
}

if (cmd === 'replicate') {
  var u = url.parse(process.argv[3])
  var req = http.request({method: 'POST', host: u.hostname, port: u.port})
  var rs = db.createReplicationStream()

  rs.on('pull', function () {
    console.log('pulled a change')
  })

  rs.on('push', function () {
    console.log('pushed a change')
  })

  rs.pipe(req)
  req.on('error', function () {})
  req.on('response', function (res) {
    res.pipe(rs)
  })
  return
}