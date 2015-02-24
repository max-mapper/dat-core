var thunky = require('thunky')
var subleveldown = require('subleveldown')
var mutexify = require('mutexify')
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
  this.lock = mutexify()

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

    self.lock(function (release) {
      dat.log.add(self.state.head, messages.Commit.encode(put), function (err, node) {
        if (err) return release(cb, err)
        dat._flush(node, function (err) {
          if (err) return release(cb, err)
          release(cb, null, node) // node is not correct per the api but for now it's ok
        })
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

Dataset.prototype.get = function (key, opts, cb) {
  if (typeof opts === 'function') return this.get(key, null, opts)
  if (!cb) cb = noop

  this.open(function (err, self, dat) {
    if (err) return cb(err)
    if (!self.state) return cb(notFound(key))

    self.db.get('0!' + key, function (err, hash) {
      if (err) return cb(err)
      dat.log.get(hash, function (err, node) {
        if (err) return cb(err)
        cb(null, messages.Commit.decode(node.value).value)
      })
    })
  })
}

module.exports = Dataset
