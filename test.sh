#!/bin/sh
#
# Validates the codebase: type-check -> lint -> test.
#
#   sh ./test.sh
#
# The integration tests in src/mod_test.ts need the brokers from
# docker-compose.yml (redis 6379 / nats 4222 / iggy 8090). Each test probes its
# port and is SKIPPED when the broker is down, so this script passes with or
# without docker. For full coverage start the brokers first:
#
#   docker compose up -d && sh ./test.sh
#
set -e

echo "==> Type-checking"
deno check src/mod_test.ts src/mod.ts src/redis/mod.ts src/jetstream/mod.ts src/iggy/mod.ts

echo "==> Linting"
deno lint src/

echo "==> Testing"
deno test -A src/

echo "==> OK"
