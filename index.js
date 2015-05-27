var events = require('events')
var xtend = require('xtend')
var duplexify = require('duplexify')
var union = require('sorted-union-stream')
var diff = require('sorted-diff-stream')
var util = require('util')
var lexint = require('lexicographic-integer')
var collect = require('stream-collector')
var thunky = require('thunky')
var mutexify = require('mutexify')
var path = require('path')
var pump = require('pump')
var through = require('through2')
var mkdirp = require('mkdirp')
var leveldown = require('leveldown-prebuilt')
var errors = require('level-errors')
var fs = require('fs')
var encoding = require('./lib/encoding')
var indexer = require('./lib/indexer')
var messages = require('./lib/messages')
var dataset = require('./lib/dataset')

var PUT = messages.TYPE.PUT
var DELETE = messages.TYPE.DELETE
var ROW = messages.CONTENT.ROW
var FILE = messages.CONTENT.FILE

var noop = function () {}
var getLayers = function (index, key, cb) {
  var result = []

  var loop = function (key) {
    index.layers.get(key, function (err, layer) {
      if (err) return cb(err)
      index.log.get(key, function (err, keyNode) {
        if (err) return cb(err)
        index.log.get(layer, function (err, layerNode) {
          if (err) return cb(err)
          result.push([keyNode.change, layerNode.key])
          if (!layerNode.links.length) return cb(null, result) // root node
          loop(layerNode.links[0])
        })
      })
    })
  }

  loop(key)
}

var Dat = function (dir, opts) {
  if (!(this instanceof Dat)) return new Dat(dir, opts)
  if (!opts) opts = {}

  if (typeof dir === 'object' && dir) {
    opts.db = dir
    dir = null
  }
  if (dir) dir = path.resolve(dir)

  events.EventEmitter.call(this)

  this.valueEncoding = opts.valueEncoding || opts.encoding || 'binary'
  this.head = null
  this.meta = null
  this.opened = false

  this._encoding = encoding(this.valueEncoding)
  this._layers = []
  this._layerChange = 0
  this._layerKey = null
  this._checkout = false
  this._lock = mutexify()
  this._index = null

  var self = this

  this.open = thunky(function (cb) {
    var oncheckout = function (head) {
      getLayers(self._index, head, function (err, layers) {
        if (err) return cb(err)

        self.head = head
        self._layers = layers
        self._layerChange = layers[0][0]
        self._layerKey = layers[0][1]

        cb(null, self)
      })
    }

    var onparent = function () {
      opts.parent.open(function (err) {
        if (err) return cb(err)
        self._index = opts.parent._index
        onindex()
      })
    }

    var onlayer = function (layer) {
      self._index.heads.get(layer, function (err, head) {
        if (err) return cb(err)
        oncheckout(head)
      })
    }

    var onindex = function () {
      self.meta = self._index.meta // expose

      if (opts.checkout) {
        self._checkout = true
        oncheckout(opts.checkout)
        return
      }

      if (opts.layer) {
        self._checkout = true
        onlayer(opts.layer)
        return
      }

      if (self._index.mainLayer) onlayer(self._index.mainLayer)
      else cb(null, self)
    }

    var datPath = dir && path.join(dir, '.dat')

    var ondb = function (db) {
      self._index = indexer({db: db, path: datPath, blobs: opts.blobs}, function (err) {
        if (err) return cb(err)
        onindex()
      })
    }

    var onbackend = function (backend) {
      self._index = indexer({path: datPath, backend: backend, blobs: opts.blobs, multiprocess: opts.multiprocess !== false}, function (err) {
        if (err) return cb(err)
        onindex()
      })
    }

    if (opts.db) return ondb(opts.db)
    if (opts.parent) return onparent()

    if (!path) return cb(new Error('Invalid path'))

    fs.exists(datPath, function (exists) {
      if (exists) return onbackend(opts.backend || leveldown)
      if (!opts.createIfMissing) return cb(new Error('No dat here'))

      mkdirp(datPath, function (err) {
        if (err) return cb(err)
        onbackend(opts.backend || leveldown)
      })
    })
  })

  this.open(function (err) {
    if (err) return self.emit('error', err)
    self.opened = true
    self.emit('ready')
  })
}

util.inherits(Dat, events.EventEmitter)

Dat.prototype.dataset = function (name) {
  return dataset(name, this)
}

Dat.prototype.heads = function (cb) {
  if (!this.opened) return this._createProxyStream(this.heads, [cb])

  var self = this
  var write = function (key, enc, cb) {
    self._index.merges.get(key, function (err, merged) {
      if (err && !err.notFound) return cb(err)
      cb(null, merged ? null : key)
    })
  }

  return collect(pump(this._index.heads.createValueStream(), through.obj(write)), cb)
}

Dat.prototype.layers = function (cb) {
  if (!this.opened) return this._createProxyStream(this.layers, [cb])
  return collect(this._index.heads.createKeyStream(), cb)
}

Dat.prototype.layer = function (layer, opts) {
  return new Dat(null, xtend({parent: this, valueEncoding: this.valueEncoding, layer: layer}, opts))
}

Dat.prototype.checkout = function (head, opts) {
  return new Dat(null, xtend({parent: this, valueEncoding: this.valueEncoding, checkout: head}, opts))
}

var notFound = function (key) {
  return new errors.NotFoundError('Key not found in database [' + key + ']')
}

Dat.prototype.createFileReadStream = function (key) {
  var stream = duplexify()
  var self = this

  stream.setWritable(false)
  this.get(key, function (err, result) {
    if (err) return stream.destroy(err)
    if (result.content !== 'file') return stream.destroy(new Error('Key is not a file'))
    if (!self._index.blobs) return stream.destroy(new Error('No blob store attached'))
    stream.setReadable(self._index.blobs.createReadStream(result.value.key))
  })

  return stream
}

Dat.prototype.createFileWriteStream = function (key) {
  var stream = duplexify()

  var destroy = function (err) {
    process.nextTick(function () {
      stream.destroy(err)
    })
  }

  stream.setReadable(false)
  this.open(function (err, self) {
    if (err) return stream.destroy(err)
    if (!self._index.blobs) return destroy(new Error('No blob store attached'))

    var ws = self._index.blobs.createWriteStream()

    stream.on('prefinish', function () {
      stream.cork()
      self.put(key, messages.File.encode(ws), {content: 'file'}, function (err, value) {
        if (err) return stream.destroy(err)
        stream.row = value
        stream.uncork()
      })
    })

    stream.setWritable(ws)
  })

  return stream
}

Dat.prototype.get = function (key, opts, cb) { // TODO: refactor me
  if (typeof opts === 'function') return this.get(key, null, opts)
  if (!opts) opts = {}

  key = (opts.dataset ? opts.dataset + '!' : '!') + key

  this.open(function (err, self) {
    if (err) return cb(err)
    if (!self._layerKey) return cb(notFound(key))

    var encoding = opts.encoding || self._encoding
    var onoperation = function (err, op, node) {
      if (err) return cb(err)
      cb(null, {
        content: op.content === FILE ? 'file' : 'row',
        type: op.type === PUT ? 'put' : 'del',
        version: node.key,
        change: node.change,
        file: op.file,
        key: op.key,
        value: op.value && op.content === FILE ? messages.File.decode(op.value) : encoding.decode(op.value)
      })
    }

    var onpointer = function (err, ptr) {
      if (err) return cb(err)
      if (opts.change) return self._getOperation(ptr, onoperation) // TODO: does this trigger deopt since it changes return type?
      self._getPointerEntry(ptr, cb)
    }

    // TODO: fix rc where *latest* is updated in the middle of below block
    // which can break a checkout snapshot - workaround for now is forcing a change lookup
    if (self._checkout) return self._getChangePointer(key, onpointer)

    // TODO: remove this lookup - it should be cacheable
    self._index.heads.get(self._layerKey, function (err, head) { // TODO: if the head changes once it is always changed
      if (err) return cb(err)
      if (head === self.head) return self._getLatestPointer(key, onpointer)
      self._getChangePointer(key, onpointer)
    })
  })
}

Dat.prototype._getLatestPointer = function (key, cb) { // fast track
  var self = this

  this._index.data.get('!latest!' + this._layerKey + '!' + key, function (err, ptr) {
    if (err && !err.notFound) return cb(err)
    if (err) return self._getLayersPointer(key, cb)
    cb(null, ptr)
  })
}

var getChangePointer = function (self, key, layer, change, cb) {
  var revs = self._index.data.createValueStream({
    gt: '!changes!' + layer + '!' + key + '!',
    lte: '!changes!' + layer + '!' + key + '!' + lexint.pack(change, 'hex'),
    reverse: true,
    limit: 1
  })

  collect(revs, function (err, keys) {
    if (err) return cb(err)
    cb(null, keys.length ? keys[0] : null)
  })
}

Dat.prototype._getChangePointer = function (key, cb) { // slow track
  var self = this
  getChangePointer(this, key, this._layerKey, this._layerChange, function (err, ptr) {
    if (err) return cb(err)
    if (!ptr) return self._getLayersPointer(key, cb)
    cb(null, ptr)
  })
}

Dat.prototype._getLayersPointer = function (key, cb) {
  var i = 0
  var self = this

  var loop = function (err, ptr) {
    if (err) return cb(err)
    if (ptr) return cb(null, ptr)
    if (++i >= self._layers.length) return cb(notFound(key))
    getChangePointer(self, key, self._layers[i][1], self._layers[i][0], loop)
  }

  loop(null, null)
}

Dat.prototype._getOperation = function (ptr, cb) {
  var i = ptr.lastIndexOf('!')
  var index = parseInt(ptr.slice(i + 1), 10)
  var version = ptr.slice(0, i)

  this._index.get(version, function (err, node, commit) {
    if (err) return cb(err)
    cb(null, commit.operations[index], node)
  })
}

Dat.prototype._getPointerEntry = function (ptr, cb) {
  var self = this
  this._getOperation(ptr, function (err, entry, node) {
    if (err) return cb(err)
    if (entry.type === DELETE) return cb(notFound(entry.key))
    cb(null, {
      content: entry.content === FILE ? 'file' : 'row',
      key: entry.key,
      version: node.key,
      value: entry.value && (entry.content === FILE ? messages.File.decode(entry.value) : self._encoding.decode(entry.value))
    })
  })
}

Dat.prototype.put = function (key, value, opts, cb) {
  if (typeof opts === 'function') return this.put(key, value, null, opts)
  if (!opts) opts = {}
  this._commit(null, [{
    type: PUT,
    content: opts.content === 'file' ? FILE : ROW,
    dataset: opts.dataset,
    key: key,
    value: Buffer.isBuffer(value) ? value : this._encoding.encode(value)
  }], cb)
}

Dat.prototype.del = function (key, opts, cb) {
  if (typeof opts === 'function') return this.del(key, null, opts)
  if (!opts) opts = {}
  this._commit(null, [{type: DELETE, dataset: opts.dataset, key: key}], cb)
}

Dat.prototype.batch = function (batch, opts, cb) {
  if (typeof opts === 'function') return this.batch(batch, null, opts)
  if (!opts) opts = {}

  var operations = new Array(batch.length)
  for (var i = 0; i < batch.length; i++) {
    var b = batch[i]
    operations[i] = {
      type: b.type === 'del' ? DELETE : PUT,
      dataset: opts.dataset,
      key: b.key,
      value: b.value && (Buffer.isBuffer(b.value) ? b.value : this._encoding.encode(b.value))
    }
  }
  this._commit(null, operations, cb)
}

Dat.prototype._commit = function (links, operations, cb) {
  if (!cb) cb = noop
  this.open(function (err, self) {
    if (err) return cb(err)
    self._lock(function (release) {
      self._index.add(links || self.head, {operations: operations}, function (err, node, layer) {
        if (err) return release(cb, err)
        if (links && (links[0] !== self.head && links[1] !== self.head)) return release(cb, null, node.key)

        if (!self._layerKey || self._layerKey !== layer) {
          self._layers.unshift([node.change, layer])
          self._layerKey = layer
        }

        self._layerChange = self._layers[0][0] = node.change
        self.head = node.key

        release(cb, null, node.key)
      })
    })
  })
}

Dat.prototype.flush = function (cb) {
  this.open(function (err, self) {
    if (err) return cb(err)
    self._index.flush(function (err) {
      cb(err)
    })
  })
}

Dat.prototype.changes =
Dat.prototype.createChangesStream = function (opts) {
  if (!this.opened) return this._createProxyStream(this.createChangesStream, [opts])
  return this._index.log.createReadStream(opts)
}

Dat.prototype.createWriteStream = function (opts) {
  if (!this.opened) return this._createProxyStream(this.createWriteStream, [opts])
  if (!opts) opts = {}
  var self = this

  var toOperation = function (data) {
    return {
      type: data.type === 'del' ? DELETE : PUT,
      dataset: opts.dataset,
      key: data.key,
      value: self._encoding.encode(data.value)
    }
  }

  return through.obj(function (data, enc, cb) {
    self._commit(null, Array.isArray(data) ? data.map(toOperation) : [toOperation(data)], function (err) {
      cb(err)
    })
  })
}

Dat.prototype.diff =
Dat.prototype.createDiffStream = function (headA, headB, opts) {
  var self = this
  var a = this.checkout(headA)
  var b = this.checkout(headB)

  var binaryEncoding = encoding('binary')
  var findFork = function () {
    // TODO: we probably don't need a double loop here
    // since the layers are sorted bottom -> top
    // but we have "few" layers so its probably fine for now
    for (var i = 0; i < a._layers.length; i++) {
      for (var j = 0; j < b._layers.length; j++) {
        if (a._layers[i][1] === b._layers[j][1]) {
          return Math.min(a._layers[i][0], b._layers[j][0])
        }
      }
    }
    return 0 // nothing shared
  }

  var fork = -1
  var filter = function (data, enc, cb) {
    if (fork === -1) fork = findFork()
    var a = data[0]
    var b = data[1]
    if (a && a.change < fork) a = data[0] = null
    if (b && b.change < fork) b = data[1] = null
    if (!a && !b) return cb()
    if (!a && b.type === 'del') return cb()
    if (!b && a.type === 'del') return cb()
    if (a) a.value = self._encoding.decode(a.value)
    if (b) b.value = self._encoding.decode(b.value)
    cb(null, data)
  }

  var isEqual = function (a, b, cb) {
    if (a.change === b.change) return cb(null, true)
    if (a.value.length !== b.value.length) return cb(null, false)
    for (var i = 0; i < a.value.length; i++) {
      if (b.value[i] !== a.value[i]) return cb(null, false)
    }
    cb(null, true)
  }

  var readOpts = {change: true, encoding: binaryEncoding}

  return pump(diff(a.createReadStream(readOpts), b.createReadStream(readOpts), isEqual), through.obj(filter))
}

Dat.prototype.merge =
Dat.prototype.createMergeStream = function (headA, headB) {
  if (!this.opened) return this._createProxyStream(this.createMergeStream, [headA, headB])
  if (!headA && !headB) throw new Error('You need to provide two nodes')

  var self = this
  var operations = []
  var stream = duplexify.obj()

  var write = function (data, enc, cb) {
    operations.push({
      type: data.type === 'del' ? DELETE : PUT,
      dataset: data.dataset,
      key: data.key,
      value: self._encoding.encode(data.value)
    })

    cb()
  }

  stream.on('prefinish', function () {
    stream.cork()
    self._commit([headA, headB], operations, function (err, head) {
      if (err) return stream.destroy(err)
      stream.head = head
      stream.uncork()
    })
  })

  stream.setReadable(false)
  stream.setWritable(through.obj(write))

  return stream
}

Dat.prototype.createKeyStream = function (opts) {
  if (!opts) opts = {}
  opts.keys = true
  opts.values = false
  return this.createReadStream(opts)
}

Dat.prototype.createValueStream = function (opts) {
  if (!opts) opts = {}
  opts.keys = false
  opts.values = true
  return this.createReadStream(opts)
}

var toKey = function (data) {
  return data.key.slice(data.key.lastIndexOf('!') + 1)
}

var emptyStream = function () {
  var stream = through.obj()
  stream.end()
  return stream
}

Dat.prototype.createReadStream = function (opts) {
  if (!this.opened) return this._createProxyStream(this.createReadStream, [opts])
  if (!opts) opts = {}

  var self = this
  var justKeys = opts.keys && !opts.values
  var justValues = opts.values && !opts.keys

  if (!this._layers.length) return emptyStream()

  var stream = this._layers
    .map(function (layer) {
      var prefix = '!latest!' + layer[1] + '!' + (opts.dataset || '') + '!'
      var rs = self._index.data.createReadStream({
        gt: prefix + (opts.gt || ''),
        lt: prefix + (opts.lt || '\xff'),
        lte: opts.lte ? prefix + opts.lte : undefined,
        gte: opts.gte ? prefix + opts.gte : undefined,
        limit: opts.limit
      })

      return rs
    })
    .reduce(function (a, b) {
      return union(a, b, toKey)
    })

  var write = function (data, enc, cb) {
    var key = data.key.slice(data.key.lastIndexOf('!') + 1)

    self.get(key, opts, function (err, row) {
      if (err && err.notFound) return cb()
      if (err) return cb(err)
      if (justKeys) return cb(null, key)
      if (justValues) return cb(null, row.value)
      cb(null, row)
    })
  }

  return pump(stream, through.obj(write))
}

Dat.prototype.createReplicationStream =
Dat.prototype.replicate = function (opts) {
  if (!this.opened) return this._createProxyStream(this.replicate, [opts])

  var self = this
  var finalize = function (cb) {
    self.flush(cb)
  }

  return this._index.log.replicate(xtend(opts, {finalize: finalize}))
}

Dat.prototype.createPullStream =
Dat.prototype.pull = function (opts) {
  if (!opts) opts = {}
  opts.mode = 'pull'
  return this.replicate(opts)
}

Dat.prototype.createPushStream =
Dat.prototype.push = function (opts) {
  if (!opts) opts = {}
  opts.mode = 'push'
  return this.replicate(opts)
}

Dat.prototype._createProxyStream = function (method, args) {
  var proxy = duplexify.obj()

  this.open(function (err, self) {
    if (err) return proxy.destroy(err)
    if (proxy.destroyed) return proxy.destroy()

    var stream = method.apply(self, args)

    stream.on('pull', function (value) {
      proxy.emit('pull', value)
    })

    stream.on('push', function (value) {
      proxy.emit('push', value)
    })

    stream.on('metadata', function (value) {
      proxy.emit('metadata', value)
    })

    proxy.setReadable(!!stream.readable && stream)
    proxy.setWritable(!!stream.writable && stream)
  })

  return proxy
}

module.exports = Dat
