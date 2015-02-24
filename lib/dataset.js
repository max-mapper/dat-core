var thunky = require('thunky')
var subleveldown = require('subleveldown')
var mutexify = require('mutexify')
var lexint = require('lexicographic-integer')
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

  var init = function () {
    if (!branch) {
      branch = Object.keys(dat._index[name] || {}).reduce(function (result, key) {
        if (!result) return key
        return dat._index[name][result].root.change > dat._index[name][key].root.change ? key : result
      }, null)
    }

    self.state = dat._index[name] && dat._index[name][branch]
    if (self.state) self.db = subleveldown(dat._data, self.state.root.hash)
  }

  this.open = thunky(function (cb) {
    dat.open(function (err) {
      if (err) return cb(err)
      init()
      cb(null, self, dat)
    })
  })

  this.openWrite = thunky(function (cb) {
    self.open(function (err, self, dat) {
      if (err) return cb(err)
      if (self.db) return cb(null, self, dat)

      var init = {type: 'init', dataset: name}

      dat.log.add(null, messages.Commit.encode(init), function (err, node) {
        if (err) return cb(err)

        dat._flush(node, function (err) {
          if (err) return cb(err)
          init()
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

  if (this._checkout) throw new Error('put on a checkout is currently not implmented')

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
    dat.log.add(self.state.head, messages.Commit.encode(node), function (err, node) {
      if (err) return release(cb, err)
      dat._flush(node, function (err) {
        if (err) return release(cb, err)
        release(cb, null, node) // node is not correct per the api but for now it's ok
      })
    })
  })
}

var notFound = function (key) {
  var err = new Error(key + 'not found') // todo: fix error
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
      dat.log.get(hash, function (err, node) {
        if (err) return cb(err)
        cb(null, messages.Commit.decode(node.value).value)
      })
    })
  })
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
      self.dat.log.get(hash, function (err, node) {
        if (err) return cb(err)
        cb(null, messages.Commit.decode(node.value).value)
      })
    })
  })
}

module.exports = Dataset
