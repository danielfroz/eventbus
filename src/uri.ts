import { ArgumentError } from './Errors.ts'

/** Connection parts parsed from a URI by {@link parseUri}. */
export interface ParsedUri {
  /**
   * Lowercased scheme without the trailing ':' (eg. 'tcp', 'tls'), or undefined
   * when the URI carried no scheme.
   */
  protocol?: string
  hostname: string
  port?: number
  username?: string
  password?: string
}

const SCHEME = /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//

/**
 * Parses a connection URI into its parts. The scheme is **optional** — both
 * `tcp://iggy:iggy@localhost:8090` and `localhost:8090` are accepted. Missing
 * parts are left `undefined` so callers can apply their own defaults
 * (`url.port`, `url.username`, `url.password`).
 */
export function parseUri(uri: string): ParsedUri {
  if (!uri || !uri.trim())
    throw new ArgumentError('uri')
  const hasScheme = SCHEME.test(uri)
  // a placeholder scheme lets WHATWG URL parse a scheme-less authority
  const url = new URL(hasScheme ? uri : `eventbus://${uri}`)
  const hostname = url.hostname
  if (!hostname)
    throw new ArgumentError('uri.hostname')
  return {
    protocol: hasScheme ? url.protocol.replace(/:$/, '').toLowerCase() : undefined,
    hostname,
    port: url.port ? Number(url.port) : undefined,
    username: url.username ? decodeURIComponent(url.username) : undefined,
    password: url.password ? decodeURIComponent(url.password) : undefined,
  }
}
