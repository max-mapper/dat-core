We store mutable data in [leveldown](https://github.com/Level/leveldown), a key/value store with basic iterator support. We store data such that:

- changes are stored in a [graph](https://github.com/jbenet/random-ideas/issues/20) that uses a technique we call 'layers' to support multiple forks of a dataset
- each user keeps track of what changes they made, so that when replication happens users can compare their state with the remote user
- data can be prefixed/namespaced to support multiple datasets in a single repository
- metadata in the graph is dereferenced from actual data (content addressed), so you can replicate the graph very quickly and choose the appropriate transport for the actual data

## logs

We use 'user' to refer to a clone of a repository being replicated amongst peers in a distributed system.

When user A writes data, their change is recorded in an append only log, e.g.:

``
userA.put('foo', 'bar')
=> inserts foo = bar into the graph, key is a hash of the change, e.g. cb22b433hf7233
=> appends 'userA-changelog-1 = cb22b433hf7233'
``

Subsequent changes made by userA will be recorded as `userA-changelog-2`, `userA-changelog-3` etc

When userA replicates with userB they can compare changelogs with each other to determine which parts of the graph need to be replicated.

We love logs and recommend reading the book ["I Heart Logs, Event Data, Stream Processing, and Data Integration"](http://shop.oreilly.com/product/0636920034339.do)

## layers

We perform 'implicit branching' when conflicting edits are made in the graph. This is similar to how union filesystems (AKA copy-on-write) work.

```
a (key: foo, value: cat) (create new layer, layer-id => graph-node-id (version-id))
|
b (key: baz, value: dog) (lookup a's layer, if head insert in that layer, if not head create new layer)
|
c (key: foo, value: horse)
```

```
a
|
b
| \
c  d (create new layer, inherit from layer a (checkout at b))
```

## checkouts

Normally to read a key you just look at the "latest" version. If you do a `checkout(version id)`, you just lookup the change number of that version. This lets you read the state of the database relative to any point in it's history.

```
// get change number for version
var change = graph.get(<version id>).change

// for each get, do a range lookup relative to the change location in the graph
var value = readStream({lte: <layer><key><change>, reverse: true, limit: 1})
```

# Low level documentation

## dat-core keys

various indexes on top of the graph to support dat operations

`{"key":"!data!!changes!eb4a5151839a71ebffa3f465ef8696e7b5ce4198ea938286aee82eaada403fd7!!ak11246285!09","value":"b0c2b798af2a6ebb0b198387b04b3b8c55db63320f940d8d6f9242b49e6443aa!0"}`

`<layer id> <key> <change number> = <version id> <version index>`

one per key/layer

`{"key":"!data!!latest!eb4a5151839a71ebffa3f465ef8696e7b5ce4198ea938286aee82eaada403fd7!!ak11246285","value":"b0c2b798af2a6ebb0b198387b04b3b8c55db63320f940d8d6f9242b49e6443aa!0"}`

`<layer id> <key> = <latest version id> <version index>`

one per key/layer

`{"key":"!heads!eb4a5151839a71ebffa3f465ef8696e7b5ce4198ea938286aee82eaada403fd7","value":"b0c2b798af2a6ebb0b198387b04b3b8c55db63320f940d8d6f9242b49e6443aa"}`

`<layer id> = <latest version in layer>`

one per layer

`{"key":"!layers!27e87a28e43624bc765fe4350f5e31d8546098192318c31357f7d381224065d2","value":"eb4a5151839a71ebffa3f465ef8696e7b5ce4198ea938286aee82eaada403fd7"}`

`<version id> = <layer id>`

one per version

## graph keys

managed by hyperlog. totally immutable

`{"key":"!log!!changes!01","value":"eb4a5151839a71ebffa3f465ef8696e7b5ce4198ea938286aee82eaada403fd7"}`

`<change number> = <version id>`

`{"key":"!log!!heads!b0c2b798af2a6ebb0b198387b04b3b8c55db63320f940d8d6f9242b49e6443aa","value":"b0c2b798af2a6ebb0b198387b04b3b8c55db63320f940d8d6f9242b49e6443aa"}`

`<head id> (sublevel of all heads, value is irrelevant)`

`{"key":"!log!!logs!cia8nsrhq0000lkbx8y2hjmfx!01","value":"\b\u0001\u0012@eb4a5151839a71ebffa3f465ef8696e7b5ce4198ea938286aee82eaada403fd7"}`

`<peer id> <peer change number> = <protobuf with replication stuff>`

`{"key":"!log!!nodes!27e87a28e43624bc765fe4350f5e31d8546098192318c31357f7d381224065d2","value":"\b\u0004\u0012@27e87a28e43624bc765fe4350f5e31d8546098192318c31357f7d381224065d2\u001a\u0019cia8nsrhq0000lkbx8y2hjmfx \u0004*�\u0002\u0010�����)\n�\u0002\b\u0001\u001a\nnc72212216\"�\u0002{\"time\":\"2014-04-30T02:57:51.800Z\",\"latitude\":\"37.3798\",\"longitude\":\"-122.1912\",\"depth\":\"4.5\",\"mag\":\"1.3\",\"magType\":\"Md\",\"nst\":\"8\",\"gap\":\"104.4\",\"dmin\":\"0.01796631\",\"rms\":\"0.02\",\"net\":\"nc\",\"id\":\"nc72212216\",\"updated\":\"2014-04-30T03:28:05.271Z\",\"place\":\"2km SSE of Ladera, California\",\"type\":\"earthquake\"}2@3aa272884ce0698985924b642eaabc6bfce83a671e629bd17faf6bd5135d6736"}`

`<version id> = <protobuf that contains data as well as links to parents>`

## meta keys

used by dat for various caching things like dat status

`{"key":"!meta!changes","value":"9"}`
`{"key":"!meta!layer","value":"eb4a5151839a71ebffa3f465ef8696e7b5ce4198ea938286aee82eaada403fd7"}`
`{"key":"!meta!log","value":"/Users/maxogden/Desktop/dat-test/.dat:cia8nsrhq0000lkbx8y2hjmfx"}`
