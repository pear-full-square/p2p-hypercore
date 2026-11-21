# test/

The tests of p2p-topic, a section per file, each test by its name.

## `topic.test.js`

- a topic of the store's own: records appended, read by offset, the same topic by the same name
- a replica: read only, empty until replicated, then fetched up to what the writer has
- a keypair of the store's: derived by name, the same each time
- a topic's encoding is the handler it is given: a record to bytes and back
