// deno-lint-ignore-file no-explicit-any
import { assert } from 'asserts'
import { ArgumentError, InitError } from './mod.ts'

Deno.test('ArgumentError check', () => {
  try {
    throw new ArgumentError('This is an failed argument')
  }
  catch(error: Error|any) {
    assert(error.message === 'This is an failed argument')
    assert(error.toString().includes('ArgumentError'), "ArgumentError type not included")
  }
})

Deno.test('InitError name check', () => {
  try {
    throw new InitError('This is an error')
  }
  catch(error: Error|any) {
    assert(error.toString().includes('InitError'))
  }
})