// Third-party dependencies for the Iggy adapter. Kept here (and intentionally
// out of deno.json#imports) so only consumers of `@danielfroz/eventbus/iggy`
// ever resolve the apache-iggy client.
export { Client, Consumer, Partitioning, PollingStrategy } from 'npm:apache-iggy@0.8.0'
