// deno-lint-ignore-file no-explicit-any
import { Client, Consumer, Partitioning, PollingStrategy } from './deps.ts'
import type { Config, Event, EventBus, EventHandler } from '../mod.ts'
import { ArgumentError, EventHandlerError, InitError, NetworkError } from '../mod.ts'
import { parseUri } from '../uri.ts'

/**
 * CompressionAlgorithm.None as understood by the Iggy binary protocol.
 * (1 = None, 2 = Gzip)
 */
const COMPRESSION_NONE = 1
/**
 * Iggy organizes data as stream -> topic -> partitions. ACTT maps one
 * service ("producer") to one Iggy stream and funnels every event of that
 * service through a single topic. This is the name of that topic.
 */
const TOPIC = 'events'

export interface EventBusIggyConfig {
  /**
   * Connection URI, eg. `tcp://iggy:iggy@localhost:8090`. The scheme selects the
   * transport: `tcp://` → TCP (default), `tls://` → TLS. Missing port defaults
   * to 8090; missing credentials default to `iggy`/`iggy`.
   */
  uri: string
  /**
   * Number of partitions created for the producer topic. Defaults to 1
   * (single partition preserves global ordering, mirroring Redis Streams /
   * Jetstream behaviour).
   */
  partitions?: number
  /**
   * Maximum amount of messages fetched per poll cycle. Defaults to 100.
   */
  batch?: number
  /**
   * Poll interval in milliseconds. Defaults to 500.
   */
  interval?: number
  /**
   * Enables Iggy client debug tracing.
   */
  trace?: boolean
}

type ConsumeSource = {
  stream: string
  topic: string
  group: string
}

export class EventBusIggy implements EventBus {
  private iconfig?: Config
  private config: EventBusIggyConfig
  /**
   * Connection details parsed from config.uri
   */
  private host: string
  private port: number
  private transport: 'TCP' | 'TLS'
  private username: string
  private password: string
  /**
   * Single pooled Iggy client used for both publishing and polling.
   */
  private client?: Client
  private handlers = new Map<string, EventHandler<Event>>()
  private sources = new Array<ConsumeSource>()
  private interval?: ReturnType<typeof setInterval>
  /**
   * Indicates that a poll cycle is currently in flight (avoids stacking runs).
   */
  private running = false

  constructor(config: EventBusIggyConfig) {
    if(!config)
      throw new ArgumentError('config')
    if(!config.uri)
      throw new ArgumentError('config.uri')

    this.config = config
    this.config.partitions = this.config.partitions ?? 1
    this.config.batch = this.config.batch ?? 100
    this.config.interval = this.config.interval ?? 500
    this.config.trace = this.config.trace ?? false

    const u = parseUri(config.uri)
    this.host = u.hostname
    this.port = u.port ?? 8090
    this.transport = u.protocol === 'tls' ? 'TLS' : 'TCP'
    this.username = u.username ?? 'iggy'
    this.password = u.password ?? 'iggy'
  }

  async init(config: Config): Promise<void> {
    if(config == null)
      throw new ArgumentError('config')
    if(!config.producer)
      throw new ArgumentError('config.producer')
    if(!config.error)
      throw new ArgumentError('config.error')
    if(!config.instance)
      config.instance = `${config.producer}.${Math.floor(Date.now() / 1000)}`

    this.iconfig = config
    const { producer, instance } = config

    try {
      this.client = new Client({
        transport: this.transport,
        options: { host: this.host, port: this.port } as any,
        credentials: { username: this.username, password: this.password },
      })
      // producer setup: ensure our own stream + topic exist so we can publish.
      // The first command (stream.get) lazily connects + authenticates and so
      // doubles as a fail-fast connectivity/credentials check. We deliberately
      // do NOT call system.ping()/getStats() here: those are zero-payload
      // commands and the underlying client serializes them via Buffer.fill(),
      // which throws under Deno's node:Buffer polyfill for empty payloads.
      await this._ensureStream(producer)
      await this._ensureTopic(producer)
    }
    catch(error: Error | any) {
      throw new NetworkError({
        producer,
        instance,
        message: `EventBusIggy; host: ${this.host}:${this.port} (${this.transport}); init failed: ${error.message}`,
        stack: `${error.stack}`,
      })
    }

    // publisher only? then we are done
    if(!config.consuming) {
      return
    }

    // consumers require both an errorHandler and handlers
    if(!config.errorHandler) {
      throw new InitError('config.errorHandler.required')
    }
    if(!config.handlers) {
      throw new InitError('config.handlers.required')
    }

    // initialize handlers
    for(const hop of config.handlers) {
      const handler = typeof(hop) === 'function' ? await hop() : hop
      if(!handler.type)
        throw new InitError('handler.type required (declare a `type` field or use @Consumes(type))')
      this.handlers.set(handler.type, handler)
    }

    // for each consuming source, ensure the stream/topic exist (idempotent,
    // works even before the remote producer published anything) and create +
    // join a consumer group named after *this* service (producer).
    for(const stream of config.consuming) {
      await this._ensureStream(stream)
      await this._ensureTopic(stream)
      await this._ensureGroup(stream, producer)
      this.sources.push({ stream, topic: TOPIC, group: producer })
    }

    this.running = false
    this.interval = setInterval(() => this._poll(config), this.config.interval)
  }

  private async _poll(config: Config): Promise<void> {
    if(this.sources.length === 0) {
      this.running = false
      return
    }
    // avoid stacking multiple runs due to setInterval
    if(this.running)
      return
    this.running = true

    const { producer, instance } = config

    try {
      for(const source of this.sources) {
        const { stream, topic, group } = source

        const handleError = async (args: { message: string, event?: Event, stack?: string }) => {
          const { message, event, stack } = args
          if(config.errorHandler) {
            await config.errorHandler(new EventHandlerError({
              message,
              stream,
              producer,
              event,
              stack,
            }))
          }
        }

        let response
        try {
          response = await this.client!.message.poll({
            streamId: stream,
            topicId: topic,
            consumer: Consumer.Group(group),
            partitionId: 0, // ignored for group consumers (server assigns)
            pollingStrategy: PollingStrategy.Next,
            count: this.config.batch!,
            autocommit: false,
          })
        }
        catch(error: Error | any) {
          const nerror = new NetworkError({ producer, instance: instance!, message: error.message, stack: `${error.stack}` })
          if(config.error)
            await config.error(nerror)
          continue
        }

        if(!response || !response.messages || response.messages.length === 0) {
          continue
        }

        for(const message of response.messages) {
          // mirrors the ack semantics of the Redis/Jetstream buses: the offset
          // is advanced after the handler runs (success OR routed to the
          // errorHandler), so a crash mid-handle redelivers on the next poll.
          const commit = async () => {
            await this.client!.offset.store({
              streamId: stream,
              topicId: topic,
              consumer: Consumer.Group(group),
              partitionId: null, // null for group consumers
              offset: message.headers.offset,
            })
          }

          const content = new TextDecoder().decode(message.payload)
          if(content == null || content.length === 0) {
            await handleError({ message: 'message.content.required' })
            await commit()
            continue
          }

          const event = config.decode ?
            await config.decode(content) :
            JSON.parse(content) as Event

          if(!event) {
            await handleError({ message: 'event.required' })
            await commit()
            continue
          }
          if(!event.type) {
            await handleError({ message: 'event.type.required', event })
            await commit()
            continue
          }
          if(!event.sid) {
            await handleError({ message: 'event.sid.required' })
            await commit()
            continue
          }
          if(!event.id) {
            await handleError({ message: 'event.id.required', event })
            await commit()
            continue
          }
          if(!event.ts) {
            await handleError({ message: 'event.ts.required', event })
            await commit()
            continue
          }

          const handler = this.handlers.get(event.type)
          if(!handler) {
            if(config.log)
              config.log.trace({ msg: `no handler for event: ${event.type}` })
            await commit()
            continue
          }

          if(config.log) {
            config.log.trace({ msg: 'exec handler', stream, instance, handler: handler.constructor.name, event })
          }

          await handler.handle(event)
            .catch(async (err: Error) => {
              await handleError({ message: err.message, stack: `${err.stack}`, event })
            })
            .finally(async () => {
              await commit()
            })
        } // !for message
      } // !for source
    }
    finally {
      this.running = false
    }
  }

  /**
   * Iggy's get* commands resolve to null/undefined for a missing entity rather
   * than throwing, so existence is decided by the returned value (a thrown
   * error is treated as "missing" too, then surfaced by the create attempt).
   */
  private async _exists(get: Promise<unknown>): Promise<boolean> {
    try {
      return !!(await get)
    }
    catch {
      return false
    }
  }

  private async _ensureStream(name: string): Promise<void> {
    if(await this._exists(this.client!.stream.get({ streamId: name })))
      return
    try {
      // streamId is auto-assigned by the server; reference everything by name
      await this.client!.stream.create({ name })
    }
    catch(error: Error | any) {
      // tolerate races: another instance may have created it concurrently
      if(!this._alreadyExists(error))
        throw this._network(error)
    }
  }

  private async _ensureTopic(stream: string): Promise<void> {
    if(await this._exists(this.client!.topic.get({ streamId: stream, topicId: TOPIC })))
      return
    try {
      await this.client!.topic.create({
        streamId: stream,
        name: TOPIC,
        partitionCount: this.config.partitions!,
        compressionAlgorithm: COMPRESSION_NONE,
      })
    }
    catch(error: Error | any) {
      if(!this._alreadyExists(error))
        throw this._network(error)
    }
  }

  private async _ensureGroup(stream: string, group: string): Promise<void> {
    if(!(await this._exists(this.client!.group.get({ streamId: stream, topicId: TOPIC, groupId: group })))) {
      try {
        await this.client!.group.create({
          streamId: stream,
          topicId: TOPIC,
          name: group,
        })
      }
      catch(error: Error | any) {
        if(!this._alreadyExists(error))
          throw this._network(error)
      }
    }
    // joining is idempotent; required so this instance becomes a group member
    try {
      await this.client!.group.join({ streamId: stream, topicId: TOPIC, groupId: group })
    }
    catch(error: Error | any) {
      if(!this._alreadyExists(error))
        throw this._network(error)
    }
  }

  private _alreadyExists(error: Error | any): boolean {
    const msg = `${error?.message ?? error}`.toLowerCase()
    return msg.includes('already exists') || msg.includes('already_exists') || msg.includes('already a member')
  }

  private _network(error: Error | any): NetworkError {
    const config = this.iconfig!
    return new NetworkError({
      producer: config.producer,
      instance: config.instance!,
      message: error.message,
      stack: `${error.stack}`,
    })
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

    if(this.client)
      await this.client.destroy()
  }

  async publish(event: Event): Promise<void> {
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

    if(!this.client)
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
      await config.encode(event) :
      JSON.stringify(event)

    try {
      await this.client.message.send({
        streamId: producer,
        topicId: TOPIC,
        messages: [{ payload: content }],
        partition: Partitioning.Balanced,
      })
    }
    catch(error: Error | any) {
      const nerror = new NetworkError({
        producer,
        instance,
        message: error.message,
        stack: `${error.stack}`,
      })
      if(config.error)
        await config.error(nerror)
      else
        throw nerror
    }
  }
}
