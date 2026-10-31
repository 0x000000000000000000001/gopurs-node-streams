# purescript-node-streams

## Local Go development

This checkout is part of the gopurs library family. Use the
[local Go development guide](../gopurs/README.md#develop-one-library-locally)
for toolchain setup, sibling dependencies, Spago configuration and Go commands.
The existing npm, Bower and Dhall commands below retain their JavaScript or
upstream roles.

The native `pipe` implementation forwards source `data` events into the actual
destination writer, then finishes the destination on source `end` unless
`pipe'` selects `{ end: false }`. It reports write/short-write/close errors,
uses `CloseWrite` for duplex sockets when available, and does not start a second
reader for existing network/HTTPS producers. Default file streams use the
paired `gopurs-node-fs` adapters. Register completion/error listeners before
starting a native pipe.

`bin/test` first runs `test/pipe-native.mjs`, with the real emitter and compiler
runtime sources. Set `GOPURS_NATIVE_TEST_RACE=1` for Go race detection. These
regressions cover actual bytes, empty/multichunk files, errors and termination;
the historical gzip/passThrough callback tests alone do not prove delivery.
General `read`, pause/resume, unpipe, pipeline and transform implementations are
outside this fix. A file producer may continue reading to EOF after a pipe
detaches on destination error; no general producer cancellation is claimed.


[![Latest release](http://img.shields.io/github/release/purescript-node/purescript-node-streams.svg)](https://github.com/purescript-node/purescript-node-streams/releases)
[![Build status](https://github.com/purescript-node/purescript-node-streams/workflows/CI/badge.svg?branch=master)](https://github.com/purescript-node/purescript-node-streams/actions?query=workflow%3ACI+branch%3Amaster)
[![Pursuit](https://pursuit.purescript.org/packages/purescript-node-streams/badge)](https://pursuit.purescript.org/packages/purescript-node-streams)

A wrapper for Node's [Stream API](https://nodejs.org/api/stream.html).

See the `example` directory for a usage example.

## Installation

```
spago install node-streams
```

## Documentation

Module documentation is [published on Pursuit](http://pursuit.purescript.org/packages/purescript-node-streams).
