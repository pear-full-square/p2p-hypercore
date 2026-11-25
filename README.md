# p2p-hypercore

Hypercores of records for the p2p layer: one writer each, appended, read by offset.

One of the p2p series. A core is a Hypercore of records: one writer, appended only: a
storage container, not a core (a core is an mrepo's, logical). A store's own core is
writable, by name; another store's is a replica, opened by its key and
filled by replication over whatever stream the application opens.

## Links

- [test](test/)

## Usage

```js
import { hypercores } from 'p2p-hypercore'
const t = hypercores(store.corestore)               // a Corestore: p2p-store's
const own = await t.own('registry')             // this store's, writable
await own.append({ op: 'register' }, { key })   // -> its offset
await own.read({ from: 0 })                     // [{ offset, key, timestamp, headers, value }]
const replica = await t.replica(own.key)        // another store's, by its key
await replica.fetch({ length })                 // brought up to that length
const identity = await t.keyPair('node')        // a keypair of this store's
```

- **A record**: `{ key, value, timestamp, headers }`, Kafka's names; its
  offset is its place in the core.
- **Its encoding is a handler's**, `{ encode, decode }`, given with the
  store: `hypercores(corestore, { handler })`; JSON
  (`mycelium-value-json`) when none. Another encoding, AVRO, is another
  handler: nothing of a core's users changes.
- **`fetch({ length, timeout })`**: the length is what the writer said it
  has; not there in time: `not fetched`. An update asked while no peer is
  connected yet answers at once, so it is asked again until it is there.
- **`read`** answers what is held: a record a replica has not taken yet
  is left out, not waited for.
- **A keypair** is derived from the store's primary key and a name, as
  an own core's is: the same each time the store is opened.

## Tests

- `brittle-bare test/*.test.js` (Bare installed): each test by its name in [test/](test/).
