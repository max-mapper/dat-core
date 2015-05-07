var sublevel = require('subleveldown')
var pump = require('pump')
var through = require('through2')
var events = require('events')
var util = require('util')
var cuid = require('cuid')
var lexint = require('lexicographic-integer')
var hyperlog = require('hyperlog')
var levelup = require('levelup')
var path = require('path')
var messages = require('./messages')
var multiprocess = require('./multiprocess')

var noop = function () {}

var loop = function (indexer, live, cb) {
  var indexNode = function (data, layer, batch, cb) {
    var commit = messages.Commit.decode(data.value)
    var operations = commit.operations

    for (var i = 0; i < operations.length; i++) {
      var op = operations[i]
      var ptr = data.key + '!' + i
      batch.push({type: 'put', key: '!data!!changes!' + layer + '!' + op.dataset + '!' + op.key + '!' + lexint.pack(data.change, 'hex'), value: ptr})
      batch.push({type: 'put', key: '!data!!latest!' + layer + '!' + op.dataset + '!' + op.key, value: ptr})
    }

    batch.push({type: 'put', key: '!layers!' + data.key, value: layer})
    batch.push({type: 'put', key: '!heads!' + layer, value: data.key})
    batch.push({type: 'put', key: '!meta!changes', value: data.change.toString()})

    indexer.db.batch(batch, function (err) {
      if (err) return cb(err)
      cb(null, data, layer)
    })
  }

  var getLayer = function (key, data, cb) {
    indexer.layers.get(key, function (err, layer) {
      if (err) return cb(err)
      indexer.heads.get(layer, function (err, head) {
        if (err) return cb(err)
        if (head === key) cb(null, layer)
        else cb(null, data.key)
      })
    })
  }

  var onmerge = function (data, batch, cb) {
    if (data.links.length > 2) return cb(new Error('three-way merge not implemented'))

    var headA = data.links[0]
    var headB = data.links[1] // not used

    batch.push({type: 'put', key: '!merges!' + headA, value: data.key})
    batch.push({type: 'put', key: '!merges!' + headB, value: data.key})

    getLayer(headA, data, function (err, layer1) {
      if (err) return cb(err)
      getLayer(headB, data, function (err, layer2) {
        if (err) return cb(err)

        if (layer1 !== data.key) return indexNode(data, layer1, batch, cb)
        if (layer2 !== data.key) return indexNode(data, layer2, batch, cb)

        indexNode(data, layer1, batch, cb)
      })
    })
  }

  var oninit = function (data, batch, cb) {
    if (!indexer.mainLayer) {
      indexer.mainLayer = data.key
      batch.push({type: 'put', key: '!meta!layer', value: data.key})
    }
    indexNode(data, data.key, batch, cb)
  }

  var onmultiprocessupdate = function (err, node, layer) {
    if (!indexer.multiprocess) return
    indexer.multiprocess.update({
      changes: indexer.changes,
      mainLayer: indexer.mainLayer,
      error: err && err.message,
      layer: layer,
      node: node
    })
  }

  var write = function (data, enc, cb) {
    indexer.changes = data.change

    var batch = []
    var done = function (err, node, layer) {
      while (indexer._pending.length && indexer._pending[0][0] === data.change) {
        var pending = indexer._pending.shift()
        if (err) pending[2](err)
        else pending[2](null, node, layer)
      }

      onmultiprocessupdate(err, node, layer)
      cb(err)
    }

    if (data.links.length > 1) return onmerge(data, batch, done)
    if (data.links.length === 0) return oninit(data, batch, done)

    getLayer(data.links[0], data, function (err, layer) {
      if (err) return cb(err)
      indexNode(data, layer, batch, done)
    })
  }

  indexer.meta.get('changes', function (_, changes) {
    indexer.changes = parseInt(changes || 0, 10)
    pump(indexer.log.createReadStream({live: live, since: indexer.changes}), through.obj(write), cb)
  })
}

var samePath = function (a, b) {
  return (!a && !b) || a === b
}

var Indexer = function (opts, cb) {
  if (!(this instanceof Indexer)) return new Indexer(opts, cb)

  if (typeof opts === 'function') {
    cb = opts
    opts = null
  }
  if (!cb) cb = noop

  events.EventEmitter.call(this)
  this.setMaxListeners(0)

  var self = this

  var createLog = function (db) {
    var meta = sublevel(db, 'meta')

    var getId = function (cb) {
      meta.get('log', function (err, log) {
        if (err && !err.notFound) return cb(err)

        var onlog = function () {
          cb(null, log.split(':').pop())
        }

        if (log && samePath(log.split(':').slice(0, -1).join(':'), opts.path)) return onlog()
        log = (opts.path || '') + ':' + cuid()
        meta.put('log', log, onlog)
      })
    }

    return hyperlog(sublevel(db, 'log'), {getId: getId})
  }

  var createDb = function () {
    return levelup(path.join(opts.path, 'db'), {db: opts.backend})
  }

  var once = true
  var ready = function (db, log, leader) {
    self.db = db
    self.data = sublevel(db, 'data')
    self.layers = sublevel(db, 'layers')
    self.heads = sublevel(db, 'heads')
    self.meta = sublevel(db, 'meta')
    self.merges = sublevel(db, 'merges')
    self.log = log
    self.leader = leader
    self.emit('update')

    var done = function (err) {
      if (leader && self.multiprocess) self.multiprocess.update(self)
      if (!once) return
      once = false
      process.nextTick(function () {
        cb(err)
      })
    }

    if (leader) return self.start(done)
    if (!opts.multiprocess) return done(new Error('not in multiprocess mode'))

    if (self.multiprocess.follower) return done()
    self.multiprocess.once('follower', done)
  }

  this.mainLayer = null
  this.changes = 0

  this.db = null
  this.data = null
  this.layers = null
  this.heads = null
  this.meta = null
  this.merges = null
  this.log = null
  this.leader = false

  this._pending = []
  this._looping = false

  if (opts.db) {
    ready(opts.db, createLog(opts.db), true)
    return
  }

  if (!opts.multiprocess && opts.backend || !opts.path) {
    var db = levelup(path.join(opts.path, 'db'), {db: opts.backend})
    ready(db, createLog(db), true)
    return
  }

  this.multiprocess = multiprocess({
    sockPath: process.platform === 'win32' ? '\\\\.\\pipe\\dat\\' + path.resolve(opts.path) : path.join(opts.path, 'dat.sock'),
    log: createLog,
    db: createDb
  })

  this.multiprocess.on('database', function () {
    if (!self.mainLayer) self.mainLayer = this.mainLayer
    self.changes = this.changes
    ready(this.db, this.log, this.leader)
  })

  this.multiprocess.on('update', function (update) {
    if (!self.mainLayer) self.mainLayer = update.mainLayer
    self.changes = this.changes

    var err = update.error && new Error(update.error)
    var layer = update.layer
    var node = update.node
    var pending

    while (self._pending.length && self._pending[0][0] < self.changes) {
      pending = self._pending.shift()
      self._flushNode(pending[1], pending[2])
    }

    while (self._pending.length && self._pending[0][0] === self.changes) {
      pending = self._pending.shift()
      if (err) pending[2](err)
      else pending[2](null, node, layer)
    }
  })
}

util.inherits(Indexer, events.EventEmitter)

Indexer.prototype.start = function (cb) {
  var self = this

  this.meta.get('layer', function (_, layer) {
    if (layer) self.mainLayer = layer
    loop(self, false, function (err) {
      if (cb) cb(err)
      loop(self, true, function (err) {
        if (err) self.emit('error', err)
      })
    })
  })
}

Indexer.prototype._flushNode = function (node, cb) {
  if (this.changes < node.change) {
    this._pending.push([node.change, node, cb])
  } else {
    this.layers.get(node.key, function (err, layer) {
      if (err) return cb(err)
      cb(null, node, layer)
    })
  }
}

Indexer.prototype.flush = function (cb) {
  if (this.changes >= this.log.changes) return cb()
  this._pending.push([this.log.changes, null, cb])
}

Indexer.prototype.add = function (links, value, cb) {
  if (!cb) cb = noop
  var self = this
  this.log.add(links, messages.Commit.encode(value), function (err, node) {
    if (err) return cb(err)
    self._flushNode(node, cb)
  })
}

Indexer.prototype.get = function (hash, cb) {
  this.log.get(hash, function (err, node) {
    if (err) return cb(err)
    cb(null, node, messages.Commit.decode(node.value))
  })
}

module.exports = Indexer
