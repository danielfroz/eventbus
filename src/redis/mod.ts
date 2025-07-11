// deno-lint-ignore-file no-explicit-any
import * as r from 'jsr:@db/redis@0.40.0'
import type { Config, Event, EventBus, EventHandler } from '../mod.ts'
import { ArgumentError, EventHandlerError, HandlerError, InitError, NetworkError } from "../mod.ts"
import type * as i from './mod.ts'

export interface EventBusRedisConfig {
  hostname: string
  port?: number | string
  username?: string
  password?: string
  trace?: boolean
}

export class EventBusRedis implements EventBus {
  private iconfig?: Config
  /**
   * Producer Redis
   */
  private predis?: r.Redis
  /**
   * Consumer Redis
   */
  private credis?: r.Redis
  /**
   * Redis configuration
   */
  private config: i.EventBusRedisConfig
  private handlers = new Map<string, EventHandler<Event>>()
  private interval?: number
  /**
   * Indicates if running 
   */
  private running?: boolean

  constructor(config: i.EventBusRedisConfig) {
    if(!config)
      throw new ArgumentError('config')
    if(!config.hostname)
      throw new ArgumentError('config.hostname')

    this.config = config
    if(!this.config.port)
      this.config.port = 6379
    this.config.trace = this.config.trace ?? false
  }

  async init(config: Config): Promise<void> {
    if(config == null)
      throw new ArgumentError('config');
    if(!config.producer)
      throw new ArgumentError('config.producer')
    if(!config.error)
      throw new ArgumentError('config.error')
    if(!config.instance)
      config.instance = `${config.producer}.${Math.floor(Date.now() / 1000)}`

    // register publisher so understand correct name
    this.iconfig = config

    const { producer, instance } = config

    const connect = async (): Promise<r.Redis> => {
      try {
        const descriptor = await r.connect({ hostname: this.config.hostname, port: this.config.port })
        if(this.config.username != null && this.config.password != null)
          await descriptor.auth(this.config.username, this.config.password)
        else if(this.config.password != null)
          await descriptor.auth(this.config.password)
        return descriptor
      }
      catch(error: Error | any) {
        throw new NetworkError({
          producer,
          instance,
          message: `EventBusRedis; configuration: ${JSON.stringify(this.config)}; connect failed: ${error}`,
          stack: `${error.stack}`
        })
      }
    }

    // this maybe a publisher only.... so 1 connection suffices
    this.predis = await connect()
    
    // the rest of the code is for consumer... if no consuming section
    // it assumes that will deal with publish() only actions...
    if(!config.consuming) {
      return
    }

    // then consumers are configured...
    // if no handler... thrown an Error
    this.credis = await connect()
    if(!config.handlers) {
      throw new ArgumentError('config.handlers')
    }

    if(config.handlers) {
      for(const handler of config.handlers) {
        const types = new Array<string>()
        if(handler.type) {
          types.push(handler.type)
        }
        for(const t of types) {
          this.handlers.set(t, handler)
        }
      }
    }
  
    // this is used when reading from groups...
    const streams = new Array<r.XKeyIdGroup>()
    if(config.consuming) {
      for(const stream of config.consuming) {
        try {
          await this.credis?.xgroupCreate(stream, name, '$', true);
        }
        catch(error: Error|any) {
          if(!error.message.includes('already exists')) {
            throw new NetworkError({ producer: name, instance, message: error.message })
          }
        }
        streams.push({ key: stream, xid: '>' })
      }
    }
  
    const throwError = (stream: string, group: string, message: string): Promise<void> => {
      if(config.error)
        config.error(new HandlerError({
          message,
          stream,
          group,
        }))
      return Promise.resolve()
    }
  
    this.running = false
    this.interval = setInterval(async () => {
      // only executes if there is streams for this EventBusRedis.
      // in other words, no need to execute when it's a publisher only
      if(!streams || streams.length == 0) {
        this.running = false
        return
      }
      
      if(this.running) {
        return;
      }

      // indicates that it's still running
      // avoids stacking multiples runs due to setInterval
      this.running = true

      const result = await this.credis?.xreadgroup(streams, { group: name, consumer: instance })
      if(!result || result.length == 0) {
        // no message to read... sleep
        this.running = false
        return 
      }

      // const promisesHandlers = new Array<{handler: EventHandler<c.Events.Event>, event: c.Events.Event}> ()
      const promises = new Array<Promise<void>>()
      for(const reply of result) {
        const stream = reply.key
        const messages = reply.messages
        for(const message of messages) {
          const content = message.fieldValues['content']
          if(content == null) {
            await throwError(stream, name, 'message.content required')
            await this.credis?.xack(stream, name, message.xid)
            continue
          }

          const event = config.decode ? 
            await config.decode(content):
            JSON.parse(content) as Event

          if(!event) {
            await throwError(stream, name, 'message.event.required')
            await this.credis?.xack(stream, name, message.xid)
            continue
          }
          if(!event.type) {
            await throwError(stream, name, 'message.event.type.required')
            await this.credis?.xack(stream, name, message.xid)
            continue
          }
          if(!event.sid) {
            await throwError(stream, name, 'message.event.sid.required')
            await this.credis?.xack(stream, name, message.xid)
            continue
          }
          if(!event.id) {
            await throwError(stream, name, 'message.event.id.required')
            await this.credis?.xack(stream, name, message.xid)
            continue;
          }
          if(!event.ts) {
            await throwError(stream, name, 'message.event.ts.required')
            await this.credis?.xack(stream, name, message.xid)
            continue
          }

          const handler = this.handlers.get(event.type)
          if(!handler) {
            // console.info(`EventBusRedis; name: ${name}, stream: ${stream}; no handler for event ${JSON.stringify(event)}`)
            await this.credis?.xack(stream, name, message.xid)
            continue
          }

          // if(this.rconfig?.trace) {
          //   console.log('eventbus [trace] consumer: %o; stream: %o; handler: %o; event: %o', 
          //     instance, stream, handler.constructor.name, event)
          // }
          
          const p = handler
            .handle(event)
            .catch((err: Error) => {
              // console.error('EventBusRedis: handler catch: %o', e)
              if(this.iconfig && this.iconfig.errorHandler) {
                this.iconfig.errorHandler(new EventHandlerError({
                  error: err,
                  event,
                  producer: handler.constructor.name,
                  stream: handler.type,
                  stack: `${err.stack}`
                }))
              }
            })
            .finally(() => {
              if(message && message.xid)
                this.credis!.xack(stream, name, message.xid)
            })
          promises.push(p)
        } // !for message
      } // !for stream

      Promise.all(promises)

      this.running = false
    }, 500)
  }

  _sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

  async destroy(): Promise<void> {
    if(this.interval)
      clearInterval(this.interval)
    
    while(true) {
      if(!this.running)
        break
      await this._sleep(100)
    }

    this.credis?.close()
    this.predis?.close()
    return Promise.resolve()
  }

  async publish(event: Event): Promise<void> {
    // checking events...
    if(!event)
      throw new ArgumentError('event')
    if(!event.type || typeof(event.type) !== 'string')
      throw new ArgumentError('event.type')
    if(!event.id)
      throw new ArgumentError('event.id')
    if(!event.sid)
      throw new ArgumentError('event.sid')
    if(!event.author)
      throw new ArgumentError('event.author')
    if(!event.ts)
      event.ts = new Date().toISOString()

    if(!this.predis)
      throw new InitError('init required')

    const config = this.iconfig
    if(!config)
      throw new InitError('config not correctly initialized')

    const { producer, instance } = config
    if(!producer)
      throw new InitError('config.producer.required')
    if(!instance)
      throw new InitError('config.instance.required')
    
    const content = config.encode ?
      await config.encode(event):
      JSON.stringify(event)
    
    try {
      await this.predis.xadd(producer, '*', { content } as r.XAddFieldValues)
    }
    catch(error: Error|any) {
      const nerror = new NetworkError({ 
        producer,
        instance,
        message: error.message,
        stack: `${error.stack}`
      })
      if(config.error)
        config.error(nerror)
      else
        throw nerror
    }
  }
}
