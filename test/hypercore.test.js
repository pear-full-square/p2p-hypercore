import test from 'brittle'
import { mkdtempSync } from 'bare-fs'
import { join } from 'bare-path'
import { tmpdir } from 'bare-os'
import Corestore from 'corestore'
import { hypercores } from '../index.js'

const dir = () => mkdtempSync(join(tmpdir(), 'p2p-hypercore-'))

test('a core of the store\'s own: records appended, read by offset, the same core by the same name', async (t) => {
  const storage = dir()
  let store = new Corestore(storage)
  let own = await hypercores(store).own('registry')
  t.is(own.writable, true)
  t.is(await own.append({ op: 'register', uri: 'swarm-1/master' }, { key: 'swarm-1/master', timestamp: '2026-11-21T00:00:00.000Z' }), 0, 'its offset')
  t.is(await own.append({ op: 'quiet', uri: 'swarm-1/node-1' }), 1)
  t.alike((await own.read())[0], { offset: 0, key: 'swarm-1/master', value: { op: 'register', uri: 'swarm-1/master' }, timestamp: '2026-11-21T00:00:00.000Z', headers: {} }, 'a record: Kafka\'s names, its offset')
  t.alike((await own.read({ from: 1 })).map(r => r.value.op), ['quiet'], 'from an offset')
  const key = own.key
  await store.close()
  store = new Corestore(storage)
  own = await hypercores(store).own('registry')
  t.alike([own.key, own.length], [key, 2], 'opened again: the same key, its records kept')
  t.not((await hypercores(store).own('log')).key, key, 'another name, another core')
  await store.close()
})

test('a replica: read only, empty until replicated, then fetched up to what the writer has', async (t) => {
  const a = new Corestore(dir())
  const b = new Corestore(dir())
  const own = await hypercores(a).own('log')
  await own.append({ n: 1 })
  await own.append({ n: 2 })
  const replica = await hypercores(b).replica(own.key)
  t.alike([replica.writable, replica.length], [false, 0])
  await t.exception(replica.fetch({ length: 2, timeout: 300 }), /not fetched in 300 ms/, 'no replication open: not fetched, said')
  const s1 = a.replicate(true)
  const s2 = b.replicate(false)
  s1.pipe(s2).pipe(s1)
  t.is(await replica.fetch({ length: 2 }), 2, 'fetched: its length')
  t.alike((await replica.read()).map(r => r.value.n), [1, 2])
  await own.append({ n: 3 })
  t.is(await replica.fetch({ length: 3 }), 3, 'again: what was appended since')
  s1.destroy(); s2.destroy()
  await a.close(); await b.close()
})

test('a keypair of the store\'s: derived by name, the same each time', async (t) => {
  const storage = dir()
  let store = new Corestore(storage)
  const k = await hypercores(store).keyPair('node')
  await store.close()
  store = new Corestore(storage)
  t.alike((await hypercores(store).keyPair('node')).publicKey, k.publicKey)
  t.unlike((await hypercores(store).keyPair('other')).publicKey, k.publicKey)
  await store.close()
})

test('a core\'s encoding is the handler it is given: a record to bytes and back', async (t) => {
  const store = new Corestore(dir())
  const seen = []
  const handler = { encode: r => { seen.push('encode'); return Buffer.from(`!${JSON.stringify(r)}`) }, decode: b => { seen.push('decode'); return JSON.parse(b.toString().slice(1)) } }
  const own = await hypercores(store, { handler }).own('typed')
  await own.append({ n: 1 }, { key: 'k' })
  t.is((await own.core.get(0)).toString()[0], '!', 'stored as the handler wrote it')
  t.alike([(await own.read())[0].value, seen], [{ n: 1 }, ['encode', 'decode']], 'read back through it')
  await store.close()
})
