var duplexify = require('duplexify')
var multileveldown = require('multileveldown')
var fs = require('fs')
var pump = require('pump')
var net = require('net')
var eos = require('end-of-stream')
var events = require('events')
var streams = require('./streams')

var noop = function () {}
var interval = setInterval(noop, 1000) // hack for keeping the event loop running in live streams and rpc

var refs = 0
var incref = function () {
  if (++refs === 1) interval.ref()
}

var decref = function () {
  if (--refs === 0) interval.unref()
}

interval.unref()

module.exports = function (opts) {
  var that = new events.EventEmitter()

  var leaderEncoders = []
  var sockPath = opts.sockPath

  var client = multileveldown.client({retry: true})
  var clientAdds = []
  var clientEncode
  var leader = false
  var log
  var clientLog

  that.changes = 0
  that.mainLayer = null
  that.leader = that.follower = false

  that.update = function (update) {
    that.mainLayer = update.mainLayer
    that.changes = update.changes

    if (!leader) return

    for (var i = 0; i < leaderEncoders.length; i++) {
      leaderEncoders[i].updates(update)
    }
    if (clientLog) {
      clientLog.changes = log.changes
      clientLog.emit('add')
    }
  }

  var alloc = function () {
    var i = clientAdds.indexOf(null)
    if (i > -1) return i
    return clientAdds.push(null) - 1
  }

  var ondbchange = function (db, changes) {
    if (!leader) {
      if (!clientLog) clientLog = opts.log(db)
      log = clientLog

      var createReadStream = log.createReadStream
      log.createReadStream = function () {
        incref()
        var rs = createReadStream.apply(this, arguments)
        eos(rs, decref)
        return rs
      }

      log.add = function (links, value, opts, cb) {
        if (typeof opts === 'function') return log.add(links, value, null, opts)
        if (!opts) opts = {}
        if (!cb) cb = noop
        if (!links) links = []
        if (!Array.isArray(links)) links = [links]
        if (typeof value === 'string') value = new Buffer(value)

        var stringLinks = []
        for (var i = 0; i < links.length; i++) {
          if (typeof links[i] === 'string') stringLinks.push(links[i])
          else stringLinks.push(links[i].key)
        }

        var id = alloc()

        clientAdds[id] = {
          id: id,
          links: stringLinks,
          value: value,
          options: opts,
          cb: cb
        }

        if (clientEncode) clientEncode.adds(clientAdds[id])
      }

      if (changes) {
        log.changes = changes
        log.emit('add')
      }
    } else {
      log = opts.log(db)
    }

    var prevFollower = that.follower

    that.follower = !leader
    that.leader = leader
    that.db = db
    that.log = log
    that.emit('database')

    if (leader) that.emit('leader')
    if (!prevFollower && that.follower) that.emit('follower')
  }

  var onclientsocket = function (sock) {
    var encode = clientEncode = streams.RPCClient.encode()
    var decode = streams.RPCServer.decode()
    var stream = duplexify(decode, encode)
    var handshook = false

    decode.updates(function (update, cb) {
      if (!handshook) {
        client.connect({encode: encode, decode: decode, ref: sock})
        handshook = true
      }

      if (!that.leader) {
        that.mainLayer = update.mainLayer
        that.changes = update.changes
      }

      if (once) {
        once = false
        ondbchange(client, update.changes)
      }

      if (!that.leader) that.emit('update', update)

      if (clientLog && clientLog.changes < update.changes) {
        clientLog.changes = update.changes
        clientLog.emit('add')
      }

      cb()
    })

    decode.adds(function (res, cb) {
      var req = clientAdds[res.id]
      if (!req) return cb()
      clientAdds[res.id] = null
      while (clientAdds.length && !clientAdds[clientAdds.length - 1]) clientAdds.pop()
      if (res.error) {
        var err = new Error(res.error)
        err.notFound = res.notFound
        res.cb(err)
      } else {
        req.cb(null, res.value)
      }
      cb()
    })

    pump(sock, stream, sock)

    for (var i = 0; i < clientAdds.length; i++) {
      var req = clientAdds[i]
      if (req) encode.adds(req)
    }
  }

  var onleader = function (db) {
    leader = true
    ondbchange(db)

    log.ready(function () {
      fs.unlink(sockPath, function () {
        var server = net.createServer(function (sock) {
          var encode = streams.RPCServer.encode()
          var decode = streams.RPCClient.decode()
          var stream = duplexify(decode, encode)

          leaderEncoders.push(encode)
          eos(encode, function () {
            var i = leaderEncoders.indexOf(encode)
            if (i > -1) leaderEncoders.splice(encode, 1)
          })

          encode.updates({mainLayer: that.mainLayer, changes: that.changes})

          decode.adds(function (req, cb) {
            log.add(req.links, req.value, req.options, function (err, result) {
              encode.adds({
                id: req.id,
                error: err && err.message,
                notFound: err && err.notFound,
                value: result
              })
            })
            cb()
          })

          if (sock.unref) sock.unref()
          multileveldown.server(db, {encode: encode, decode: decode})
          pump(sock, stream, sock)
        })

        server.listen(sockPath, function () {
          server.unref()
          var down = client.db
          client.db = db.db
          if (down.isFlushed()) return
          var sock = net.connect(sockPath)
          onclientsocket(sock)
          down.flush(function () {
            sock.destroy()
          })
        })
      })
    })
  }

  var once = true
  var onfollower = function () {
    var sock = net.connect(sockPath)
    var connected = false
    sock.on('connect', function () {
      connected = true
      onclientsocket(sock)
    })

    eos(sock, function () {
      if (connected) return retry()
      setTimeout(retry, 100)
    })
  }

  var retry = function () {
    var ready = function (err) {
      db.removeListener('open', ready)
      db.removeListener('error', ready)
      if (err) return onfollower()
      onleader(db)
    }

    var db = opts.db()

    if (db.isOpen()) return ready()

    db.on('open', ready)
    db.on('error', ready)
  }

  retry()

  return that
}
