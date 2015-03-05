var hyperlog = require('hyperlog')
var thunky = require('thunky')
var mkdirp = require('mkdirp')
var levelup = require('levelup')
var through = require('through2')
var pump = require('pump')
var fs = require('fs')
var path = require('path')
var leveldown = require('leveldown-prebuilt')
var subleveldown = require('subleveldown')
var lexint = require('lexicographic-integer')
var messages = require('./lib/messages')
var duplexify = require('duplexify')
var dataset = require('./lib/dataset')

var noop = function () {}

var Dat = function (dir, opts) {
  if (!(this instanceof Dat)) return new Dat(dir, opts)
  if (!opts) opts = {}

  var self = this

  var backend = opts.backend || leveldown
  var datPath = path.join(dir, '.dat')
  var levelPath = path.join(datPath, 'db')

  this.path = datPath
  this.log = null

  this._dir = dir
  this._db = null
  this._meta = null
  this._view = null
  this._data = null
  this._branches = null
  this._index = {}
  this._open = false

  this._callbacks = {}
  this._change = 0

  this.open = thunky(function (cb) {
    fs.exists(datPath, function (exists) {
      if (!exists && !opts.createIfMissing) return cb(new Error('No dat here'))

      mkdirp(datPath, function (err) {
        if (err) return cb(err)

        self._db = opts.db || levelup(path.join(datPath, 'db'), {db: backend})
        self._view = subleveldown(self._db, 'view', {valueEncoding: 'binary'})
        self._meta = subleveldown(self._view, 'meta', {valueEncoding: 'utf-8'})
        self._data = subleveldown(self._view, 'data', {valueEncoding: 'utf-8'})
        self._branches = subleveldown(self._view, 'branches', {valueEncoding: 'utf-8'})

        self.log = hyperlog(subleveldown(self._db, 'hyperlog'))

        var write = function (data, enc, cb) {
          self.log.get(data.key, function (err, root) {
            if (err) return cb(err)
            self.log.get(data.value, function (err, head) {
              if (err) return cb(err)
              var name = messages.Commit.decode(root.value).dataset
              if (!self._index[name]) self._index[name] = {}
              self._index[name][head.key] = {head: head, root: root}
              cb()
            })
          })
        }

        var index = function () {
          pump(self._branches.createReadStream(), through.obj(write), function (err) {
            if (err) return cb(err)
            self._process(function() {
              self._process()
              self._open = true
              cb(null, self)
            })
          })
        }

        if (!opts.reset) return index()

        var del = function (key, enc, cb) {
          self._view.del(key, cb)
        }

        pump(self._view.createKeyStream(), through.obj(del), index)
      })
    })
  })

  this.set = dataset(this, 'default')
  if (opts.head) this.set.checkout(opts.head)
}

Dat.prototype.__defineGetter__('head', function () {
  return this.set.head
})

Dat.prototype.checkout = function (hash) {
  return new Dat(this._dir, {head: hash, db: this._db})
}

Dat.prototype._process = function (cb) {
  var self = this
  var index = this._index

  var key = function (sub, key) { // a bit leaky :(
    return '!' + sub + '!' + key
  }

  var process = function (node, enc, cb) {
    var value = messages.Commit.decode(node.value)
    var batch = []
      
    var set = value.dataset

    if (!set) return cb()
    if (!index[set]) index[set] = {}
    var sindex = index[set]

    if (node.links.length > 1) throw new Error('merges not implemented yet!')

    var prev = node.links.length && node.links[0]

    if (!prev || !sindex[prev]) {
      sindex[node.key] = {root: node, head: node}
    } else {
      sindex[prev].head = node
      sindex[node.key] = sindex[prev]      
      delete sindex[prev]
    }

    var rhash = sindex[node.key].root.key

    batch.push({type: 'put', key: key('branches', rhash), value: node.key})

    if (value.type === 'put') {
      batch.push({type: 'put', key: '!data!!' + rhash + '!0!' +  value.key, value: node.key})
      batch.push({type: 'put', key: '!data!!' + rhash + '!1!' +  value.key + '!' + lexint.pack(node.change, 'hex'), value: node.key})
    }

    if (value.type === 'del') {
      batch.push({type: 'put', key: '!data!!' + rhash + '!0!' +  value.key, value: ' '})
      batch.push({type: 'put', key: '!data!!' + rhash + '!1!' +  value.key + '!' + lexint.pack(node.change, 'hex'), value: ' '})      
    }

    batch.push({type: 'put', key: '!meta!change', value: '' + node.change})

    self._view.batch(batch, function (err) {
      if (err) return cb(err)

      var queued = self._callbacks[node.change]
      delete self._callbacks[node.change]
      if (queued) queued(null)

      cb()
    })
  }

  self._meta.get('change', function (err, change) {
    if (err && !err.notFound) throw err

    change = parseInt(change || 0)
    self.log.createReadStream({since: change, live: !cb}).pipe(through.obj(process)).on('finish', function () {
      if (cb) cb()
    })
  })
}

Dat.prototype._flush = function (node, cb) {
  if (this._change >= node.change) return cb()
  this._callbacks[node.change] = cb
}

Dat.prototype.branches = function (name, cb) {
  this.open(function (err, dat) {
    if (err) return cb(err)
    if (!dat._index[name]) return cb(new Error('Dataset does not exist'))

    var toBranch = function (head) {
      return dat._index[name][head].root.key
    }

    cb(null, Object.keys(dat._index[name]).map(toBranch))
  })
}

Dat.prototype.get = function () {
  this.set.get.apply(this.set, arguments)
}

Dat.prototype.put = function () {
  this.set.put.apply(this.set, arguments)
}

Dat.prototype.del = function () {
  this.set.del.apply(this.set, arguments)
}

Dat.prototype.createReadStream = function () {
  return this.set.createReadStream.apply(this.set, arguments)
}

Dat.prototype.createWriteStream = function () {
  return this.set.createWriteStream.apply(this.set, arguments)
}

Dat.prototype.createReplicationStream = function () {
  if (this._open) return this.log.createReplicationStream()

  var proxy = duplexify()

  this.open(function (err, self) {
    if (err) return proxy.destroy(err)
    var rs = self.log.createReplicationStream()
    proxy.setReadable(rs)
    proxy.setWritable(rs)
    rs.on('pull', function () {
      proxy.emit('pull')
    })
    rs.on('push', function () {
      proxy.emit('push')
    })
  })

  return proxy
}

module.exports = Dat
