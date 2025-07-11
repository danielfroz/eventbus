import { assertEquals } from 'asserts'
import * as eb from './mod.ts'

export interface User {
  id: string
  name: string
  login: string
  email: string
  mobile?: string
}

// deno-lint-ignore no-namespace
export namespace User {
  export const CREATED = 'User.Created'
  export interface Created extends eb.Event {
    type: typeof CREATED,
    user: User;
  }
  export const UPDATED = 'User.Updated'
}

class UserCreatedHandler implements eb.EventHandler<User.Created> {
  type = User.CREATED

  // deno-lint-ignore require-await
  async handle(event: User.Created): Promise<void> {
    if(event == null)
      throw new eb.ArgumentError('event')
    const db = new Map<string, User>()
    db.set(event.user.id, event.user)
  }
}

Deno.test("Event Handler creation", function() {
  const event = {
    type: User.CREATED,
    id: '123-3123',
    sid: '123-3123',
    user: {
      name: 'Daniel Froz',
      mobile: '+55 (15) 99171-3270'
    } as User,
    timestamp: new Date()
  } as User.Created
  assertEquals(event.type, User.CREATED)

  const handler = new UserCreatedHandler();
  assertEquals(handler.type, User.CREATED)
});