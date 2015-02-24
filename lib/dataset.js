var thunky = require('thunky')
var pump = require('pump')
var through = require('through2')
var subleveldown = require('subleveldown')
var mutexify = require('mutexify')
var lexint = require('lexicographic-integer')
var duplexify = require('duplexify')
var messages = require('./messages')

var noop = function () {}

var KEYS = 'k!'
var BRANCHES = 'b!'

var Dataset = function (dat, name, branch) {
  if (!(this instanceof Dataset)) return new Dataset(dat, name, branch)

  var self = this

  this.name = name
  this.branch = branch
  this.state = null
  this.db = null
  this.dat = dat
  this.lock = mutexify()

  this._checkout = null

  this.open = thunky(function (cb) {
    dat.open(function (err) {
      if (err) return cb(err)
      self._init(branch)
      cb(null, self, dat)
    })
  })

  this.openWrite = thunky(function (cb) {
    self.open(function (err, self, dat) {
      if (err) return cb(err)
      if (self.db) return cb(null, self, dat)

      dat.log.add(null, messages.Commit.encode({type: 'init', dataset: name}), function (err, node) {
        if (err) return cb(err)

        dat._flush(node, function (err) {
          if (err) return cb(err)
          self._init(branch)
          cb(null, self, dat)
        })
      })
    })
  })
}

Dataset.prototype.__defineGetter__('head', function () {
  return this.state && this.state.head.hash
})

Dataset.prototype.del = function (key, cb) {
  if (!cb) cb = noop

  this.openWrite(function (err, self, dat) {
    if (err) return cb(err)

    var del = {
      type: 'del',
      dataset: self.name,
      key: key
    }

    self._commit(dat, del, cb)
  })
}

Dataset.prototype.put = function (key, value, opts, cb) {
  if (typeof opts === 'function') return this.put(key, value, null, opts)
  if (!cb) cb = noop

  this.openWrite(function (err, self, dat) {
    if (err) return cb(err)

    var put = {
      type: 'put',
      dataset: self.name,
      key: key,
      value: value
    }

    self._commit(dat, put, cb)
  })
}

Dataset.prototype._commit = function (dat, node, cb) {
  var self = this

  this.lock(function (release) {
    var checkout = self._checkout && self._checkout !== self.state.head.hash

    var commit = function (head) {
      dat.log.add(head, messages.Commit.encode(node), function (err, node) {
        if (err) return release(cb, err)
        dat._flush(node, function (err) {
          if (err) return release(cb, err)
          if (checkout) self._init(head.hash)
          release(cb, null, node) // node is not correct per the api but for now it's ok
        })
      })
    }

    if (!checkout) return commit(self.state.head)

    self.dat.log.get(self._checkout, function (err, head) {
      if (err) return cb(err)
      commit(head)
    })
  })
}

var notFound = function (key) {
  var err = new Error(key + ' not found') // todo: fix error
  err.notFound = true
  err.status = 404
  return err
}

Dataset.prototype.checkout = function (hash) {
  this._checkout = hash
  return this
}

Dataset.prototype.get = function (key, opts, cb) {
  if (typeof opts === 'function') return this.get(key, null, opts)
  if (!cb) cb = noop
  if (!opts) opts = {}

  var checkout = opts.checkout || this._checkout

  this.open(function (err, self, dat) {
    if (err) return cb(err)
    if (!self.state) return cb(notFound(key))

    if (checkout) return self._getCheckout(key, checkout, opts, cb)

    self.db.get('0!' + key, function (err, hash) {
      if (err) return cb(err)
      if (hash === ' ') return cb(notFound(key))

      dat.log.get(hash, function (err, node) {
        if (err) return cb(err)
        cb(null, messages.Commit.decode(node.value).value)
      })
    })
  })
}

Dataset.prototype.createReadStream = function (opts) {
  if (!opts) opts = {}

  var result = duplexify.obj()

  this.open(function (err, self, dat) {
    if (err) return result.destroy(err)

    if (!self.state) {
      result.setReadable(false)
      result.setWritable(false)
      return
    }

    var prefix = '0!'
    var checkout = opts.checkout || self._checkout
    var sopts = {
      gt: prefix + (opts.gt || ''),
      lt: prefix + (opts.lt || '\xff'),
      lte: opts.lte ? prefix + opts.lte : undefined,
      gte: opts.gte ? prefix + opts.gte : undefined
    }

    var stream = checkout ? self.db.createKeyStream(sopts) : self.db.createValueStream(sopts)

    var write = checkout ? 
      function (key, enc, cb) {
        self._getCheckout(key.slice(2), checkout, opts, function (err, node) {
          if (err && !err.notFound) return cb(err)
          if (err) return cb()
          cb(null, node)
        })
      } :
      function (hash, enc, cb) {
        if (hash === ' ') return cb()
        dat.log.get(hash, function (err, node) {
          if (err) return cb(err)
          cb(null, messages.Commit.decode(node.value))
        })
      }

    result.setReadable(pump(stream, through.obj(write)))
    result.setWritable(false)
  })

  return result
}

var getOne = function (stream, cb) {
  var result = null
  stream.on('error', cb)
  stream.on('data', function (data) {
    result = data
  })
  stream.on('end', function () {
    stream.removeListener('error', cb)
    cb(null, result)
  })
}

Dataset.prototype._getCheckout = function (key, rev, opts, cb) {
  var self = this

  this.dat.log.get(rev, function (err, node) {
    if (err) return cb(err)

    var revs = self.db.createValueStream({
      gt: '1!' + key + '!',
      lte: '1!' + key + '!' + lexint.pack(node.change, 'hex'),
      reverse: true,
      limit: 1
    })

    getOne(revs, function (err, hash) {
      if (err) return cb(err)
      if (hash === ' ' || !hash) return cb(notFound(key))
      self.dat.log.get(hash, function (err, node) {
        if (err) return cb(err)
        cb(null, messages.Commit.decode(node.value))
      })
    })
  })
}

Dataset.prototype._init = function (branch) {
  var dat = this.dat
  var name = this.name

  var reduce = function (result, key) {
    if (!result) return key      
    return dat._index[name][result].root.change > dat._index[name][key].root.change ? key : result
  }

  if (!branch) branch = Object.keys(dat._index[name] || {}).reduce(reduce, null)
  this.state = dat._index[name] && dat._index[name][branch]
  if (this.state) this.db = subleveldown(dat._data, this.state.root.hash)
}

module.exports = Dataset
