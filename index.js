var events = require('events')
var framedHash = require('framed-hash')
var batcher = require('byte-stream')
var xtend = require('xtend')
var duplexify = require('duplexify')
var pumpify = require('pumpify')
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
var replicate = require('./lib/replicate')

var PUT = messages.TYPE.PUT
var DELETE = messages.TYPE.DELETE
var ROW = messages.CONTENT.ROW
var FILE = messages.CONTENT.FILE

var DATA = messages.COMMIT_TYPE.DATA
var TRANSACTION_DATA = messages.COMMIT_TYPE.TRANSACTION_DATA
var TRANSACTION_START = messages.COMMIT_TYPE.TRANSACTION_START
var TRANSACTION_END = messages.COMMIT_TYPE.TRANSACTION_END

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

    var rollback = function (err, node, commit) {
      if (err) return cb(err)
      if (commit.type === TRANSACTION_START || commit.type === TRANSACTION_DATA) {
        self._index.get(node.links[0], rollback)
      } else {
        oncheckout(node.key)
      }
    }

    var onlayer = function (layer) {
      self._index.heads.get(layer, function (err, head) {
        if (err) return cb(err)
        self._index.get(head, rollback)
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

Dat.prototype.listDatasets = function (cb) {
  this.open(function (err, self) {
    if (err) return cb(err)

    var i = 0
    var result = []

    var peek = function (layer, cb) {
      var loop = function (prev) {
        var rs = self._index.data.createKeyStream({
          gt: '!latest!' + layer + '!',
          lt: '!latest!' + layer + '!' + (prev ? prev + '!' : '\xff'),
          reverse: true,
          limit: 1
        })

        collect(rs, function (err, keys) {
          if (err) return cb(err)
          if (!keys.length) return cb()

          var dataset = keys[0].split('!')[3]
          if (!dataset) return cb(null, result)

          result.push(dataset)
          loop(dataset)
        })
      }

      loop(null)

    }

    var loop = function (err) {
      if (err) return cb(err)
      if (self._layers.length === i) return cb(null, result)
      var layer = self._layers[i++]
      var l = layer[1]

      peek(l, loop)
    }

    loop()
  })
}

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

Dat.prototype.createFileReadStream = function (key, opts) {
  var stream = duplexify()
  var self = this

  stream.setWritable(false)
  this.get(key, opts, function (err, result) {
    if (err) return stream.destroy(err)
    if (result.content !== 'file') return stream.destroy(new Error('Key is not a file'))
    if (!self._index.blobs) return stream.destroy(new Error('No blob store attached'))
    stream.setReadable(self._index.blobs.createReadStream(result.value.key))
  })

  return stream
}

Dat.prototype.createFileWriteStream = function (key, opts) {
  if (!opts) opts = {}

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
      opts.content = 'file'
      self.put(key, messages.File.encode(ws), opts, function (err, value) {
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
      console.log(err, op, node)
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
  var nodeKey = ptr.slice(0, ptr.indexOf('!'))
  var self = this

  this._index.log.get(nodeKey, function (err, node) {
    if (err) return cb(err)
    self._index.operations.get(ptr.slice(nodeKey.length + 1), {valueEncoding: 'binary'}, function (err, op) {
      if (err) return cb(err)
      cb(null, messages.Operation.decode(op), node)
    })
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

Dat.prototype.status = function (cb) {
  this.open(function (err, self) {
    if (err) return cb(err)
    if (!self.head) return cb(new Error('This dat is empty'))
    self._index.get(self.head, function (err, node) {
      if (err) return cb(err)

      var value = messages.Commit.decode(node.value)
      var datasets = {}
      var result = {
        head: self.head,
        transaction: value.type === TRANSACTION_START || value.type === TRANSACTION_DATA,
        checkout: self._checkout,
        modified: new Date(value.modified),
        datasets: 0,
        rows: 0,
        files: 0,
        versions: 0,
        size: 0
      }

      var visit = function (node) {
        var value = messages.Commit.decode(node.value)

        result.versions++
        result.size += node.value.length
        result.rows += value.puts + value.deletes
        result.files += value.files

        var rs = self._index.operations.createValueStream({
          gt: value.operations + '!',
          lt: value.operations + '!~',
          valueEncoding: 'binary'
        })

        var write = function (data, enc, cb) {
          var op = messages.Operation.decode(data)
          if (!datasets[op.dataset]) {
            datasets[op.dataset] = true
            result.datasets++
          }
          result.size += data.length
          if (op.content === FILE) result.size += messages.File.decode(op.value).size

          cb()
        }

        pump(rs, through.obj(write), function (err) {
          if (err) return cb(err)
          if (!node.links.length) return cb(null, result)

          // doesn't matter which link we visit
          self._index.get(node.links[0], function (err, node) {
            if (err) return cb(err)
            visit(node)
          })
        })
      }

      visit(node)
    })
  })
}

Dat.prototype.put = function (key, value, opts, cb) {
  if (typeof opts === 'function') return this.put(key, value, null, opts)
  if (!opts) opts = {}
  this._commit(null, DATA, [{
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
  this._commit(null, DATA, [{type: DELETE, dataset: opts.dataset, key: key}], cb)
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
  this._commit(null, DATA, operations, cb)
}

Dat.prototype._commit = function (links, type, operations, cb) {
  if (!cb) cb = noop
  this.open(function (err, self) {
    if (err) return cb(err)

    var hash = framedHash('sha256')
    var batch = new Array(operations.length)
    var puts = 0
    var deletes = 0
    var files = 0

    for (var i = 0; i < operations.length; i++) {
      var op = operations[i]
      var val = messages.Operation.encode(op)

      if (op.content === FILE) files++

      if (op.type === DELETE) deletes++
      else puts++

      hash.update(val)
      batch[i] = {
        type: 'put',
        key: lexint.pack(i, 'hex'),
        value: val
      }
    }

    var key = hash.digest('hex')
    for (var j = 0; j < operations.length; j++) batch[j].key = key + '!' + batch[j].key

    self._index.operations.batch(batch, function (err) {
      if (err) return cb(err)
      self._lock(function (release) {
        self._index.add(links || self.head, {
          type: type,
          modified: Date.now(),
          puts: puts,
          deletes: deletes,
          files: files,
          operations: key
        }, function (err, node, layer) {
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

  var self = this
  var format = function (data, enc, cb) {
    self._index.get(data.key, function (err, node, commit) {
      if (err) return cb(err)

      cb(null, {
        change: data.change,
        date: new Date(commit.modified),
        version: data.key,
        links: data.links,
        puts: commit.puts,
        deletes: commit.deletes,
        files: commit.files
      })
    })
  }

  return pump(this._index.log.createReadStream(opts), through.obj(format))
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

  var started = true
  var startTransaction = function (fn) {
    self._commit(null, TRANSACTION_START, [], function (err) {
      if (err) return fn(err)
      started = true
      startTransaction = function (fn) { fn() }
      fn()
    })
  }

  var write = function (batch, enc, cb) {
    self._commit(null, DATA, batch.map(toOperation), function (err) {
      cb(err)
    })
  }

  var writeTransaction = function (batch, enc, cb) {
    startTransaction(function (err) {
      if (err) return cb(err)
      self._commit(null, TRANSACTION_DATA, batch.map(toOperation), function (err) {
        cb(err)
      })
    })
  }

  var endTransaction = function (cb) {
    if (!started) return cb()
    self._commit(null, TRANSACTION_END, [], function (err) {
      cb(err)
    })
  }

  var writer = opts.transaction ? through.obj(writeTransaction, endTransaction) : through.obj(write)

  return pumpify.obj(batcher({limit: opts.batchSize || 128}), writer)
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

  var decode = function (obj) {
    var key = obj.key
    var i = key.indexOf('!')
    if (i > -1) {
      obj.key = key.slice(i + 1)
      obj.dataset = key.slice(0, i)
    }
    obj.value = self._encoding.decode(obj.value)
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
    if (a) decode(a)
    if (b) decode(b)
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

  var readOpts = {change: true, encoding: binaryEncoding, all: true}

  return pump(diff(a.createReadStream(readOpts), b.createReadStream(readOpts), isEqual), through.obj(filter))
}

Dat.prototype.merge =
Dat.prototype.createMergeStream = function (headA, headB) {
  if (!this.opened) return this._createProxyStream(this.createMergeStream, [headA, headB])
  if (!headA || !headB) throw new Error('You need to provide two nodes')

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
    self._commit([headA, headB], messages.COMMIT_TYPE.DATA, operations, function (err, head) {
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

var emptyStream = function () {
  var stream = through.obj()
  stream.end()
  return stream
}

var toKey = function (data) {
  return data.key.slice(data.key.lastIndexOf('!', data.key.lastIndexOf('!') + 1) + 1)
}

Dat.prototype.createReadStream = function (opts) {
  if (!this.opened) return this._createProxyStream(this.createReadStream, [opts])
  if (!opts) opts = {}

  var self = this
  var justKeys = opts.keys && !opts.values
  var justValues = opts.values && !opts.keys
  var all = !!opts.all
  var getOpts = xtend(opts)

  if (!this._layers.length) return emptyStream()

  var stream = this._layers
    .map(function (layer) {
      var prefix = '!latest!' + layer[1] + '!' + (all ? '' : (opts.dataset || '') + '!')
      var rs = self._index.data.createReadStream({
        gt: prefix + (opts.gt || ''),
        lt: prefix + (opts.lt || '\xff'),
        lte: opts.lte ? prefix + opts.lte : undefined,
        gte: opts.gte ? prefix + opts.gte : undefined
      })

      return rs
    })
    .reduce(function (a, b) {
      return union(a, b, toKey)
    })

  var write = function (data, enc, cb) {
    var i = data.key.lastIndexOf('!')
    var key = data.key.slice(i + 1)
    var dataset = data.key.slice(data.key.lastIndexOf('!', i - 1) + 1, i)

    if (dataset) getOpts.dataset = dataset
    self.get(key, getOpts, function (err, row) {
      if (err && err.notFound) return cb()
      if (err) return cb(err)
      if (justKeys) return cb(null, key)
      if (justValues) return cb(null, row.value)
      if (all && dataset) row.key = dataset + '!' + row.key
      cb(null, row)
    })
  }

  var dataStream = pump(stream, through.obj(write))
  var limit = opts.limit || -1

  if (limit === -1) return dataStream

  var limited = through.obj(function (data, enc, cb) {
    limited.push(data)
    if (!--limit) {
      cleanup()
      dataStream.destroy()
      limited.end()
    }
    cb()
  })

  var cleanup = function () {
    limited.removeListener('close', onclose)
    limited.removeListener('error', onclose)
    dataStream.removeListener('close', onclose)
    dataStream.removeListener('error', onclose)
  }

  var onclose = function (err) {
    cleanup()
    dataStream.destroy(err)
    limited.destroy(err)
  }

  limited.on('close', onclose)
  limited.on('error', onclose)
  dataStream.on('close', onclose)
  dataStream.on('error', onclose)

  return dataStream.pipe(limited)
}

Dat.prototype.createReplicationStream =
Dat.prototype.replicate = function (opts) {
  if (!this.opened) return this._createProxyStream(this.replicate, [opts])
  return replicate(this, opts)
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

    stream.on('pull', function () {
      proxy.emit('pull')
    })

    stream.on('pull-file', function (file) {
      proxy.emit('pull-file', file)
    })

    stream.on('push-file', function (file) {
      proxy.emit('push-file', file)
    })

    stream.on('push', function () {
      proxy.emit('push')
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
