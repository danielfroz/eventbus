import { assertEquals, assertThrows } from 'asserts'
import { ArgumentError } from './mod.ts'
import { parseUri } from './uri.ts'

Deno.test('parseUri: full URI with scheme, credentials, host and port', () => {
  const u = parseUri('tcp://iggy:secret@localhost:8090')
  assertEquals(u.protocol, 'tcp')
  assertEquals(u.hostname, 'localhost')
  assertEquals(u.port, 8090)
  assertEquals(u.username, 'iggy')
  assertEquals(u.password, 'secret')
})

Deno.test('parseUri: scheme-less host:port', () => {
  const u = parseUri('localhost:4222')
  assertEquals(u.protocol, undefined)
  assertEquals(u.hostname, 'localhost')
  assertEquals(u.port, 4222)
  assertEquals(u.username, undefined)
  assertEquals(u.password, undefined)
})

Deno.test('parseUri: scheme-less host only', () => {
  const u = parseUri('redis-host')
  assertEquals(u.hostname, 'redis-host')
  assertEquals(u.port, undefined)
})

Deno.test('parseUri: password-only credentials (redis style)', () => {
  const u = parseUri('redis://:p4ss@127.0.0.1:6379')
  assertEquals(u.protocol, 'redis')
  assertEquals(u.hostname, '127.0.0.1')
  assertEquals(u.port, 6379)
  assertEquals(u.username, undefined)
  assertEquals(u.password, 'p4ss')
})

Deno.test('parseUri: tls scheme is lowercased', () => {
  assertEquals(parseUri('TLS://host:8090').protocol, 'tls')
})

Deno.test('parseUri: percent-encoded credentials are decoded', () => {
  const u = parseUri('tcp://user:p%40ss@host:1')
  assertEquals(u.username, 'user')
  assertEquals(u.password, 'p@ss')
})

Deno.test('parseUri: rejects empty input', () => {
  assertThrows(() => parseUri(''), ArgumentError)
  assertThrows(() => parseUri('   '), ArgumentError)
})
