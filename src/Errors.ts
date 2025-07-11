import type { Event } from './mod.ts'

export class EventBusError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'EventBusError'
  }
}

export class ArgumentError extends EventBusError {
  constructor(message: string) {
    super(message)
    this.name = 'ArgumentError'
  }
}

/**
 * Specific to Initialization required error
 */
export class InitError extends EventBusError {
  constructor(message: string) {
    super(message)
    this.name = 'InitError'
  }
}

export type NetworkErrorArgs = {
  producer: string
  instance: string
  message: string
  stack?: string
}
export class NetworkError extends EventBusError {
  producer: string
  instance: string

  constructor(args: NetworkErrorArgs) {
    super(args.message)
    this.producer = 'NetworkError'
    this.producer = args.producer
    this.instance = args.instance
    this.stack = args.stack
  }
}

export type HandlerErrorArgs = {
  message: string
  stream: string
  group: string
  data?: unknown
}
export class HandlerError extends EventBusError {
  stream: string
  group: string
  data?: unknown

  constructor(args: HandlerErrorArgs) {
    super(args.message)
    this.name = 'HandleError'
    this.stream = args.stream
    this.group = args.group
    this.data = args.data
  }
}

export type EventHandlerErrorArgs = {
  error: Error
  event: Event
  producer: string
  stream: string
  stack: string
}
export class EventHandlerError extends EventBusError {
  event: Event
  producer: string
  stream: string
  
  constructor(readonly args: EventHandlerErrorArgs) {
    super(args.error.message)
    this.name = 'EventHandlerError'
    this.event = args.event
    this.producer = args.producer
    this.stream = args.stream
    this.stack = args.stack
  }
}