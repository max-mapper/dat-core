var cuid = require('cuid')
var messages = require('./messages')
var duplexify = require('duplexify')

var noop = function () {}

var Commit = function (dat, opts) {
  if (!(this instanceof Commit)) return new Commit(dat, opts)
  if (!opts) opts = {}

  this._dat = dat
  this._prefix = cuid() + '!'
  this.dataset = opts.dataset
}

Commit.prototype.encode = function (val) {
  return val && (Buffer.isBuffer(val) ? val : this._dat._encoding.encode(val))
}

Commit.prototype.put = function (key, val, opts, cb) {
  if (typeof opts === 'function') return this.put(key, val, null, opts)
  if (!cb) cb = noop
  if (!opts) opts = {}

  var self = this
  var dataset = this.dataset
  var prefix = this._prefix

  this._dat.open(function (err, dat) {
    if (err) return cb(err)
    dat._index.operations.put(prefix + key, messages.Operation.encode({
      type: messages.TYPE.PUT,
      content: opts.content === 'file' ? messages.CONTENT.FILE : messages.CONTENT.ROW,
      dataset: dataset,
      key: key,
      value: self.encode(val)
    }), cb)
  })
}

Commit.prototype.del = function (key, cb) {
  if (!cb) cb = noop

  var dataset = this.dataset
  var prefix = this._prefix

  this._dat.open(function (err, dat) {
    if (err) return cb(err)
    dat._index.operations.del(prefix + key, messages.Operation.encode({
      type: messages.TYPE.DELETE,
      dataset: dataset,
      key: key
    }), cb)
  })
}

Commit.prototype.batch = function (batch, cb) {
  if (!cb) cb = noop

  var self = this
  var dataset = this.dataset
  var prefix = this._prefix

  this._dat.open(function (err, dat) {
    if (err) return cb(err)
    var ops = new Array(batch.length)
    for (var i = 0; i < batch.length; i++) {
      var b = batch[i]
      ops[i] = {
        type: b.type,
        key: prefix + b.key,
        value: messages.Operation.encode({
          type: b.type === 'del' ? messages.TYPE.DELETE : messages.TYPE.PUT,
          dataset: dataset,
          key: b.key,
          value: self.encode(b.value)
        })
      }
    }
    dat._index.operations.batch(ops, cb)
  })
}

Commit.prototype.attach = function (key, opts) {
  if (!opts) opts = {}

  var self = this
  var dataset = this.dataset
  var stream = duplexify()

  var destroy = function (err) {
    process.nextTick(function () {
      stream.destroy(err)
    })
  }

  stream.setReadable(false)
  this._dat.open(function (err, dat) {
    if (err) return destroy(err)
    if (!dat._index.blobs) return destroy(new Error('No blob store attached'))

    var ws = dat._index.blobs.createWriteStream()

    stream.on('prefinish', function () {
      stream.cork()
      opts.content = 'file'
      opts.dataset = dataset
      self.put(key, messages.File.encode(ws), opts, function (err) {
        if (err) return stream.destroy(err)
        stream.uncork()
      })
    })

    stream.setWritable(ws)
  })

  return stream
}

Commit.prototype.end = function (cb) {
  if (!cb) cb = noop

  var self = this

  this._dat.open(function (err, dat) {
    if (err) return cb(err)
    console.log(self._prefix)
  })
}

module.exports = Commit
