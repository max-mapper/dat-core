var sublevel = require('subleveldown')
var pump = require('pump')
var through = require('through2')
var events = require('events')
var util = require('util')
var cuid = require('cuid')
var lexint = require('lexicographic-integer')
var hyperlog = require('hyperlog')
var messages = require('./messages')

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

  var write = function (data, enc, cb) {
    indexer.changes = data.change

    var batch = []
    var done = function (err, node, layer) {
      while (indexer._pending.length && indexer._pending[0][0] === data.change) {
        var pending = indexer._pending.shift()
        if (err) pending[1](err)
        else pending[1](null, node, layer)
      }

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
    indexer.changes = changes || 0
    pump(indexer.log.createReadStream({live: live, since: indexer.changes}), through.obj(write), cb)
  })
}

var samePath = function (a, b) {
  if (!a && !b) return true
  return a === b
}

var Indexer = function (db, opts, cb) {
  if (!(this instanceof Indexer)) return new Indexer(db, opts, cb)

  if (typeof opts === 'function') {
    cb = opts
    opts = null
  }
  if (!cb) cb = noop

  events.EventEmitter.call(this)
  this.setMaxListeners(0)

  var self = this

  this.db = db
  this.data = sublevel(db, 'data')
  this.layers = sublevel(db, 'layers')
  this.heads = sublevel(db, 'heads')
  this.meta = sublevel(db, 'meta', {valueEncoding: 'json'})
  this.merges = sublevel(db, 'merges')
  this.log = null

  this.mainLayer = null
  this.changes = 0

  this._pending = []
  this._looping = false

  this.meta.get('log', function (err, log) {
    if (err && !err.notFound) return cb(err)

    var onlog = function (err) {
      if (err) return cb(err)
      self.log = hyperlog(sublevel(db, 'log'), {id: log.id})
      self.start(cb)
    }

    if (log && samePath(log.path, opts.path)) return onlog()
    log = {path: opts.path, id: cuid()}
    self.meta.put('log', log, onlog)
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

Indexer.prototype._flush = function (change, cb) {
  if (this.changes >= change) return cb()
  this._pending.push([change, cb])
}

Indexer.prototype.flush = function (cb) {
  this._flush(this.log.changes, cb)
}

Indexer.prototype.add = function (links, value, cb) {
  if (!cb) cb = noop
  var self = this
  this.log.add(links, messages.Commit.encode(value), function (err, node) {
    if (err) return cb(err)
    self._flush(node.change, cb)
  })
}

Indexer.prototype.get = function (hash, cb) {
  this.log.get(hash, function (err, node) {
    if (err) return cb(err)
    cb(null, node, messages.Commit.decode(node.value))
  })
}

module.exports = Indexer
