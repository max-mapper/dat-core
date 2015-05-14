# dat-core

Work in progress. Will be part of [dat](https://github.com/maxogden/dat) beta.

```
npm install dat-core
```

[![build status](http://img.shields.io/travis/maxogden/dat-core.svg?style=flat)](http://travis-ci.org/maxogden/dat-core)
![dat](http://img.shields.io/badge/Development%20sponsored%20by-dat-green.svg?style=flat)

## Usage

``` js
var dat = require('dat-core')

var db = dat('./test')

db.put('hello', 'world', function () { // insert value
  db.get('hello', functoin (err, result) {
    console.log(result)   // prints result
    console.log(db.head) // the 'head' of the database graph (a hash)
  })
})
```

## API

#### `db = dat(pathOrLevelDb, [options])`

Create a new dat instance. Options include

``` js
{
  valueEncoding: 'json' | 'binary' | 'utf-8'
}
```

#### `db.head`

String containing the current head revision of the dat.
Everytime you mutate the dat this head changes.

#### `db.put(key, value, [cb])`

Insert a value into the dat

#### `db.get(key, cb)`

Get a value node from the dat

#### `db.del(key, cb)`

Delete a node from the dat

#### `set = dat.dataset(name)`

Returns a namespaced dataset (similar to a sublevel in leveldb).
If you just use `dat.put` and `dat.get` it will use the default dataset (equaivalent of doing `dat.dataset()`.

#### `stream = db.createReadStream([options])`

Stream out values of the dat

#### `stream = db.createWriteStream([options])`

Stream in values to the dat

#### `stream = db.createFileReadStream(key)`

Read a file stored under the key specified. Returns a binary read stream.

#### `stream = db.createFileWriteStream(key)`

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

## License

MIT
