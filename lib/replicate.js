var framedHash = require('framed-hash')
var collect = require('stream-collector')
var xtend = require('xtend')
var lexint = require('lexicographic-integer')
var pump = require('pump')
var pumpify = require('pumpify')
var parallel = require('parallel-transform')
var multiplex = require('multiplex')
var through = require('through2')
var after = require('after-all')
var messages = require('./messages')

var FILE = messages.CONTENT.FILE

module.exports = function (self, opts) {
  var fetchingBlobs = []
  var blobs = self._index.blobs

  var finalize = function (cb) {
    self.flush(cb)
  }

  var onoperation = function (id, stream) {
    var op = self._index.operations.createValueStream({
      gt: id + '!',
      lt: id + '!~',
      valueEncoding: 'binary'
    })

    graphOut.cork() // cork to add a bit of flow control since we won't resolve graph nodes until blob is done anyways
    pump(op, stream, function () {
      graphOut.uncork()
    })

    plex.emit('push-operation', op)
  }

  var onfile = function (id, stream) {
    if (!self._index.blobs) return stream.destroy(new Error('no blob store attached'))

    var file = self._index.blobs.createReadStream(id)

    graphOut.cork() // cork to add a bit of flow control since we won't resolve graph nodes until blob is done anyways
    pump(file, stream, function () {
      graphOut.uncork()
    })

    plex.emit('push-file', file)
  }

  var onflow = function (stream) {
    stream.on('data', function (data) {
      data = data.toString()
      if (data === 'cork') graphOut.cork()
      else graphOut.uncork()
    })
  }

  var plex = multiplex(function (stream, id) {
    if (id === 'flow') return onflow(stream)
    if (id.indexOf('operation/') > -1) return onoperation(id.slice(10), stream)
    if (id.indexOf('file/') > -1) return onfile(id.slice(5), stream)
    stream.destroy(new Error('Unknown channel'))
  })

  plex.progress = {
    puts: 0,
    deletes: 0,
    files: 0
  }

  var fetchBlob = function (key, cb) {
    if (!blobs) return cb()

    blobs.exists(key, function (_, exists) {
      if (exists) return cb()

      if (fetchingBlobs.indexOf(key) > -1) return cb() // skip
      fetchingBlobs.push(key)

      var stream = plex.createStream('file/' + key, {chunked: true})
      var file = blobs.createWriteStream()

      pump(stream, file, function (err) {
        if (err) return cb(err)
        var i = fetchingBlobs.indexOf(key)
        fetchingBlobs.splice(i, 1)
        cb()
      })

      plex.emit('pull-file', file)
    })
  }

  var hasOperation = function (key, cb) {
    if (!key) return cb(null, true)
    self._index.operations.get(key + '!00', function (err) {
      if (err) return cb(null, false)
      cb(null, true)
    })
  }

  var flowStream = plex.createStream('flow')
  var corking = false

  // we currently handle readable backpressure
  // by corking the graph stream using the flow channel.
  // we should fix the multiplex stream per channel backpressure
  // instead since this is more or less a hack
  var buf = through.obj({highWaterMark: 1000000})

  var update = function () {
    var len = Math.max(buf._readableState.length, queue._readableState.length)
    if (len > 128 && !corking) {
      corking = true
      flowStream.write('cork')
    } else if (len < 64 && corking) {
      corking = false
      flowStream.write('uncork')
    }
  }

  var queue = parallel(16, function (node, cb) {
    update()

    var commit = messages.Commit.decode(node.value)
    var hash = framedHash('sha256')

    plex.progress.puts += commit.puts
    plex.progress.deletes += commit.deletes
    plex.progress.files += commit.files
    plex.emit('progress')

    var onbatch = function (batch) {
      var wrap = new Array(batch.length)

      var next = after(function (err) {
        update()
        if (err) return cb(err)
        cb(null, node)
      })

      for (var i = 0; i < wrap.length; i++) {
        if (commit.files) {
          var op = messages.Operation.decode(batch[i])
          if (op.content === FILE) fetchBlob(messages.File.decode(op.value).key, next())
        }

        hash.update(batch[i])
        wrap[i] = {type: 'put', key: lexint.pack(i, 'hex'), value: batch[i]}
      }

      var digest = hash.digest('hex')
      if (digest !== commit.operations) return next()(new Error('Operation checksum mismatch'))

      for (var j = 0; j < wrap.length; j++) wrap[j].key = digest + '!' + wrap[j].key
      self._index.operations.batch(wrap, next())
    }

    hasOperation(commit.operations, function (err, exists) {
      if (err) return cb(err)
      if (exists) return cb(null, node)

      var op = plex.createStream('operation/' + commit.operations)

      collect(op, function (err, batch) {
        if (err) return plex.destroy(err)
        onbatch(batch)
      })
    })
  })

  var graph = self._index.log.replicate(xtend(opts, {finalize: finalize, process: pumpify.obj(buf, queue) }))
  var graphOut = plex.createStream('graph')
  var graphIn = plex.receiveStream('graph')

  pump(graphIn, graph, graphOut)

  graph.on('error', function (err) {
    plex.destroy(err)
  })

  graph.on('metadata', function (value) {
    plex.emit('metadata', value)
  })

  graph.on('push', function () {
    plex.emit('push')
  })

  graph.on('pull', function () {
    plex.emit('pull')
  })

  graph.on('finish', function () {
    plex.end()
  })

  return plex
}
