// p2p-hypercore: Hypercores of records for the p2p layer: a core holds records,
// in Kafka's sense: one partition, one writer, appended only, a record
// read by its offset.
//   const t = hypercores(corestore, { handler })   corestore: a Corestore
//                                        (p2p-store's store.corestore);
//                                        handler: its records' encoding
//   const own = await t.own(name)        a core of this store's own, its
//                                        keypair derived from the store's
//                                        primary key and the name: writable
//   const rep = await t.replica(key)     another store's core, by its key
//                                        (hex): read only, filled by
//                                        replication
//   await t.keyPair(name)                a keypair of this store's, derived
//                                        the same way: an identity
// A core:
//   core.key                 its key, hex: what a replica is opened by
//   core.writable            whether this store writes it
//   core.length              the records it has
//   await core.append(value, { key, headers })    one record -> its offset
//   await core.read({ from, to })   -> [{ offset, key, timestamp, headers, value }]
//                             the records held, from an offset up to another
//   await core.fetch({ length, timeout })   a replica brought up to the
//                             length its writer said it has, over the
//                             replication now open: its records taken
//                             -> length; not there in time: throws
// A record: { key, value, timestamp, headers }, Kafka's names; its offset
// is its place in the core.
// Its encoding is a handler's, { encode, decode }: one record to bytes
// and back; hypercores(corestore, { handler }) takes the one it is given,
// JSON (mycelium-value-json) when none: another encoding is another
// handler, not a change to a core's users.
import * as json from 'mycelium-value-json'

const hex = b => Buffer.from(b).toString('hex')

function coreOf (core, handler) {
  return {
    core,
    key: hex(core.key),
    get writable () { return core.writable },
    get length () { return core.length },
    async append (value, { key = null, headers = {}, timestamp = new Date().toISOString() } = {}) {
      const { length } = await core.append(handler.encode({ key, value, timestamp, headers }))
      return length - 1
    },
    async read ({ from = 0, to = core.length } = {}) {
      const out = []
      for (let offset = Math.max(0, from); offset < Math.min(to, core.length); offset++) {
        // (a block a replica does not hold yet: not waited for, left out)
        const bytes = await core.get(offset, { wait: false })
        if (bytes) out.push({ offset, ...handler.decode(bytes) })
      }
      return out
    },
    async fetch ({ length, timeout = 5000 } = {}) {
      // (an update answers at once while no peer is connected yet: asked
      // again until the length is there)
      for (const until = Date.now() + timeout; ;) {
        await core.update({ wait: true })
        if (core.length >= length) break
        if (Date.now() >= until) throw new Error(`not fetched in ${timeout} ms: core ${hex(core.key).slice(0, 8)} (${core.length} of ${length})`)
        await new Promise(resolve => setTimeout(resolve, 25))
      }
      if (core.length) await core.download({ start: 0, end: core.length }).done()
      return core.length
    }
  }
}

export function hypercores (corestore, { handler = json } = {}) {
  const opened = new Map()
  const open = async (id, options) => {
    if (opened.has(id)) return opened.get(id)
    const core = corestore.get(options)
    await core.ready()
    const records = coreOf(core, handler)
    opened.set(id, records)
    return records
  }
  return {
    own: name => open(`own ${name}`, { name }),
    replica: key => open(`key ${key}`, { key: Buffer.from(key, 'hex') }),
    keyPair: name => corestore.createKeyPair(name),
    opened: () => [...opened.values()]
  }
}
