# dat-core

# Work in progress. Will be part of [dat](https://github.com/maxogden/dat) beta.

```
npm install dat-core
```

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

#### `db = dat(path, [options])`

Create a new dat instance

#### `db.open([cb])`

Force open the dat. Otherwise it is opened lazily.

#### `db.head`

String containing the current head revision of the dat.
Everytime you mutate the dat this head changes.

#### `db.put(key, value, [options], [cb])`

Insert a value into the dat

#### `db.get(key, [options], cb)`

Get a value node from the dat

#### `db.del(key, [options], cb)`

Delete a node from the dat

#### `stream = db.createReadStream([options])`

Stream out values of the dat

#### `stream = db.createWriteStream([options])`

Stream in values to the dat

#### `stream = db.createPushStream([options])`

Create a replication stream that both pushes changes to another dat

#### `stream = db.createPullStream([options])`

Create a replication stream that both pulls changes from another dat

#### `stream = db.createSyncStream([options])`

Create a replication stream that both pulls and pushes

#### `anotherDat = db.checkout(ref)`

Checkout a branch of the dat or an older revision. This is useful if you want to pin your data
to a point in time.

```
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

#### `dataset = db.dataset(name)`

Create a namespaced dataset. Similar to using the `dataset` option
on puts/gets etc

```
var salaries = db.dataset('salaries')

salaries.put('hello', 'world')
salaries.get('hello', function (err, result) {
  console.log(result) // contains world
})
```

## License

MIT
