// deno-lint-ignore-file no-explicit-any
import * as NATS from 'jsr:@nats-io/nats-core@3.1.0'
import * as NATSC from 'jsr:@nats-io/transport-deno@3.1.0';
import * as NATSJ from 'jsr:@nats-io/jetstream@3.1.0'
import { Errors } from '../mod.ts'
import type { Config, Event, EventBus, EventHandler } from '../mod.ts'

export interface EventBusJetstreamConfig {
  servers: string[]
}

export class EventBusJetstream implements EventBus {
  private iconfig?: Config
  private ncs?: NATS.NatsConnection
  private ncc?: NATS.NatsConnection
  private jss?: NATSJ.JetStreamClient
  private jsc?: NATSJ.JetStreamClient
  private subj?: string
  private running: boolean
  private intervals?: Array<number>
  private handlers = new Map<string, EventHandler<Event>>()

  constructor(private readonly jscfg: EventBusJetstreamConfig) {
    this.running = false
  }

  async connect(): Promise<NATS.NatsConnection> {
    const CONFIG = this.jscfg
    try {
      return await NATSC.connect({
        servers: CONFIG.servers
      })
    }
    catch(err: Error|any) {
      throw new Errors.InitError(`connect failed: ${JSON.stringify(this.jscfg.servers)}; err: ${err.message}`)
    }
  }

  async _initPublisher(config: Config): Promise<void> {
    try {
      const name = config.producer
      this.ncs = await this.connect()
      this.jss = await NATSJ.jetstream(this.ncs)
      this.subj = name
      const jsm = await NATSJ.jetstreamManager(this.ncs)
      const streams = await jsm.streams.list().next()
      const exists = streams.find(si => si.config.name === name)
      if(!exists) {
        await jsm.streams.add({
          name: name,
          subjects: [ name ],
          description: `stream of ${name}`,
          retention: NATSJ.RetentionPolicy.Limits,
          discard: NATSJ.DiscardPolicy.Old,
          max_age: NATS.nanos(1000 * 3600 * 24)
        })
      }
    }
    catch(err: Error|any) {
      console.log(`eventbus-jetstream: err caught: `, err.stack)
      throw new Errors.NetworkError({
        producer: config.producer,
        instance: `${config.producer}.${Math.round(new Date().getTime() / 1000)}`,
        message: `error while creating service: ${err.message}`
      })
    }
  }

  async _initConsumers(config: Config): Promise<void> {
    if(!config.consuming) {
      // nothing to be done here...
      return
    }
    
    if(!config.handlers) {
      throw new Errors.InitError(`consuming defined but no handler was set`)
    }
    
    try {
      const name = config.producer
      // initialize headers
      for(const h of config.handlers) {
        this.handlers.set(h.type, h)
      }

      this.ncc = await this.connect()
      this.jsc = NATSJ.jetstream(this.ncc)
      const jsm = await NATSJ.jetstreamManager(this.ncc) 

      const existingStreams = new Set<string>()
      const streams = await jsm.streams.list().next()
      for(const si of streams) {
        existingStreams.add(si.config.name)
      }

      // consumer setup...
      for(const stream of config.consuming) {
        if(!existingStreams.has(stream)) {
          continue
        }
        const cis = await jsm.consumers.list(stream).next()
        const existing = cis?.find(x => x.config.name === name)
        if(!existing) {
          // create consumer for this stream
          await jsm.consumers.add(stream, {
            name,
            durable_name: name,
            filter_subject: `${stream}`,
            ack_policy: NATSJ.AckPolicy.Explicit,
            deliver_policy: NATSJ.DeliverPolicy.New,
          })
        }
      }

      const consumers = new Array<{ stream: string, consumer: NATSJ.Consumer }>()
      for(const stream of config.consuming) {
        if(!existingStreams.has(stream)) {
          continue
        }
        const consumer = await this.jsc.consumers.get(stream, name)
        consumers.push({ stream, consumer })
      }

      const handlerError = (args: Errors.HandlerErrorArgs): Promise<void> => {
        return new Promise((resolve, _r) => {
          if(config.error)
            config.error(new Errors.HandlerError(args))
          return resolve()
        })
      }

      this.intervals = new Array<number>()
      this.intervals.push(setInterval(async () => {
        this.running = true
        try {
          for(const c of consumers) {
            const { stream, consumer } = c

            const msgs = await consumer.fetch({ expires: 2000 })
            for await (const msg of msgs) {

              const json = new TextDecoder().decode(msg.data)
              const event = config.decoder ? 
                await config.decoder(json):
                JSON.parse(json) as Event

              if(event == null) {
                await handlerError({ stream, group: name, message: 'event.invalid', data: undefined })
                continue
              }
              if(!event.type) {
                await handlerError({ stream, group: name, message: 'event.type.invalid', data: event })
                continue
              }
              if(!event.sid) {
                await handlerError({ stream, group: name, message: 'event.sid.invalid', data: event })
                continue
              }
              if(!event.id) {
                await handlerError({ stream, group: name, message: 'event.id.invalid', data: event })
                continue
              }
              if(!event.ts) {
                await handlerError({ stream, group: name, message: 'event.ts.invalid', data: event })
                continue
              }
              const handler = this.handlers.get(event.type)
              if(!handler) {
                continue
              }
              handler.handle(event)
                .then(() => msg.ack())
                .catch((err: Error) => {
                  if(config.errorHandler) {
                    config.errorHandler(new Errors.EventHandlerError({
                      error: err,
                      event,
                      producer: handler.constructor.name,
                      stream: handler.type,
                      stack: `${err.stack}`,
                    }))
                  }
                })
                .finally(() => {
                  msg.ack()
                })
            }
          } // !for
        }
        finally {
          this.running = false
        }
      }, 1000))
    }
    catch(err) {
      throw err
    }
  }
  
  async init(config: Config): Promise<void> {
    if(!config)
      throw new Errors.ArgumentError('config')
    this.iconfig = config
    try {
      await this._initPublisher(config)
      await this._initConsumers(config)
    }
    catch(err) {
      throw err
    }
  }

  async destroy(): Promise<void> {
    if(this.intervals) {
      for(const i of this.intervals) {
        clearInterval(i)
      }
    }
    const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))
    if(this.ncs) {
      await this.ncs.close()
    }
    if(this.ncc) {
      while(true) {
        if(!this.running)
          break
        await sleep(100)
      }
      await this.ncc.close()
    }
  }

  async publish(event: Event): Promise<void> {
    if(!event)
      throw new Errors.ArgumentError('event')
    if(!event.type || typeof(event.type) !== 'string')
      throw new Errors.ArgumentError('event.type')
    if(!event.id)
      throw new Errors.ArgumentError('event.id')
    if(!event.sid)
      throw new Errors.ArgumentError('event.sid')
    if(!event.author)
      throw new Errors.ArgumentError('event.author')
    if(!event.ts)
      event.ts = new Date().toISOString()

    if(!this.iconfig) {
      throw new Errors.InitError('eventbus not initialized')
    }
    if(!this.jss) {
      throw new Errors.InitError('not connected or initialized')
    }
    if(!this.subj) {
      throw new Errors.InitError('not initialized; this.subj')
    }

    const config = this.iconfig

    const payload = config.encoder ?
      await config.encoder(event):
      JSON.stringify(event)

    await this.jss.publish(this.subj, payload)
  }
}
