# dat-core

The core implementation of dat

```
npm install dat-core
```

[![build status](http://img.shields.io/travis/maxogden/dat-core.svg?style=flat)](http://travis-ci.org/maxogden/dat-core)
![dat](http://img.shields.io/badge/Development%20sponsored%20by-dat-green.svg?style=flat)

## Usage

``` js
var dat = require('dat-core')

var db = dat('./test')

db.put('hello', 'world', function (err) { // insert value
  if (err) return handle(err) // something went wrong
  db.get('hello', function (err, result) {
    if (err) return handle(err) // something went wrong
    console.log(result)   // prints result
    console.log(db.head) // the 'head' of the database graph (a hash)
  })
})
```

## API

#### `db = dat(pathOrLevelDb, [options])`

Create a new dat instance.

##### Options

- `checkout` - database version to access. default is latest
- `valueEncoding` - 'json' | 'binary' | 'utf-8' or a custom encoder instance
- `createIfMissing` - true or false, default false. creates dat folder if it doesnt exist
- `backend` - a leveldown compatible constructor to use (default is require('leveldown'))
- `blobs` - an abstract-blob-store compatible instance to use (default is content-addressable-blob-store)

Per default the path passed to the backend is `{path}/.dat/db`.
If your custom backend requires a special url simply wrap in a function

``` js
var sqldown = require('sqldown')
var db = dat('/some/regular/path', {
  backend: function () {
    return sqldown('pg://localhost/database')
  }
})
```

#### `db.head`

String property containing the current head revision of the dat.
Everytime you mutate the dat this head changes.

#### `db.init([cb])`

Inits the dat by adding a root node to the graph if one hasn't been added already.
Is called implicitly when you do a mutating operation.

`cb` (if specified) will be called with one argument, `(error)`

#### `db.put(key, value, [opts], [cb])`

Insert a value into the dat

`cb` (if specified) will be called with one argument, `(error)`

##### Options

- `dataset` - the dataset to use
- `valueEncoding` - an encoder instance to use to encode the value

#### `db.get(key, [options], cb)`

Get a value node from the dat

`cb` will be called with two arguments, `(error, value)`. If successful, `value` will have these keys:

```
{
  content:  // 'file' or 'row'
  type:     // 'put' or 'del'
  version:  // version hash
  change:   // internal change number
  key:      // row key
  value:    // row value
}
```

##### Options

- `dataset` - the dataset to use
- `valueEncoding` - an encoder instance to use to decode the value

#### `db.del(key, [cb])`

Delete a node from the dat by key

`cb` (if specified) will be called with one argument, `(error)`

#### `db.listDatasets(cb)`

Returns a list of the datasets currently in use in this checkout

`cb` will be called with two arguments, `(error, datasets)` where `datasets` is an array of strings (dataset names)

#### `set = dat.dataset(name)`

Returns a namespaced dataset (similar to a sublevel in leveldb).
If you just use `dat.put` and `dat.get` it will use the default dataset (equaivalent of doing `dat.dataset()`.

#### `stream = db.createReadStream([options])`

Stream out values of the dat. Returns a readable stream.

#### `stream = db.createWriteStream([options])`

Stream in values to the dat. Returns a writable stream.

##### Options

- `dataset` - the dataset to store the data in
- `message` - a human readable message string to store with the metadata for the changes made by the write stream
- `transaction` - boolean, default false. if true everything written to the write stream will be stored as 1 transaction in the history
- `batchSize` - default `128`, the group size used to write to the underlying leveldown batch write. this also determines how many nodes end up in the graph (higher batch size = less nodes)
- `valueEncoding` - override the value encoding set on the dat-core instance

##### Data format

When you write data to the write stream, it must look like this:

```
{
  type:     // 'put' or 'del'
  key:      // key
  value:    // value
}
```

#### `stream = db.createFileReadStream(key, [options])`

Read a file stored under the key specified. Returns a binary read stream.

#### `stream = db.createFileWriteStream(key, [options])`

Write a file to be stored under the key specified. Returns a binary write stream.

#### `stream = db.createPushStream([options])`

Create a replication stream that both pushes changes to another dat

#### `stream = db.createPullStream([options])`

Create a replication stream that both pulls changes from another dat

#### `stream = db.createReplicationStream([options])`

Create a replication stream that both pulls and pushes

#### `stream = db.createChangesStream([options])`

Get a stream of changes happening to the dat. These changes
are ONLY guaranteed to be ordered locally.

#### `stream = db.heads()`

Get a stream of heads in the underlying dat graph.

#### `stream = db.layers()`

Get a stream of layers in the dat.

A layer will added if both you and a remote make changes to the dat
and you then pull the remote's changes.

They can also happen if you checkout a prevision revision and make changes.

#### `stream = db.diff(branch1, branch2)`

Compare two or more branches with each other.
The stream will emit key,value pairs that conflict across the branches

#### `stream = db.merge(branch1, branch2)`

Returns a merge stream. You should write key,value pairs to this stream
that conflicts across the branches (see the compare method above).

Once you end this stream the branches will be merged assuming the don't
contain conflicting keys anymore.

#### `db.createFork(opts, cb)`

Create a new fork that is a copy of the current head. This will not checkout to new fork, just copy and return. `opts.head` can be set to fork a different head

`cb()` will be called with two arguments: `err` and `head`, which is the version of the new node that was created.

For example, starting with this graph:

```
a
|
b
|
c (latest)
```

After running createFork on `c`, where d is a copy of c:

```
a
|
b
| \
c  d <- (d is the same as c, but c is still 'latest')
```

This is equivalent to (but much faster than) running the following code because it just copies the node in the internal graph, including copying a pointer to the underlying row operations:

```
dat checkout b
dat export -d dataset --checkout=c | dat import -d dataset
```

#### `anotherDat = db.checkout(ref)`

Checkout an older revision of the dat.
This is useful if you want to pin your data to a point in time.

``` js
db.put('hello', 'world', function () {
  var oldHash = db.head
  db.put('hello', 'verden', function () {
    var oldDb = db.checkout(oldHash)

    oldDb.get('hello', function (err, result) {
      console.log(result) // contains 'hello' -> 'world'
    })

    db.get('hello', function (err, result) {
      console.log(result) // contains 'hello' -> 'verden'
    })
  })
})
```

If you want to make this checkout persistent, i.e. your default head, set the `{persistent: true}` option

``` js
var anotherDat = db.checkout(someHead, {persistent: true})

anotherDat.on('ready', function () {
  // someHash if your default head now if you create a new dat instance
})
```

To reset your persistent head to the previous use `db.checkout(false, {persistent: true})`

## Custom Encoders

Wherever you can specify `valueEncoding`, in addition to the built in string types you can also pass in an object with `encode` and `decode` methods.

For example, here is the implementation of the built-in JSON encoder:

```js
var json = {
  encode: function (obj) {
    return new Buffer(JSON.stringify(obj))
  },
  decode: function (buf) {
    return JSON.parse(buf.toString())
  }
}
```

## License

MIT
