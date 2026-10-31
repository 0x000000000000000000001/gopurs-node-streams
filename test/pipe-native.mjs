import { execFileSync } from 'node:child_process';
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const runtimeRoot = process.env.GOPURS_ROOT ?? fileURLToPath(new URL('../../gopurs/', import.meta.url));
const libraryRoot = process.env.GOPURS_LIBRARY_ROOT ?? fileURLToPath(new URL('../../', import.meta.url));
const workspace = mkdtempSync(join(tmpdir(), 'gopurs-stream-pipe-'));
try {
  mkdirSync(join(workspace, 'gopurs_runtime'));
  copyFileSync(join(runtimeRoot, 'runtime/runtime.go'), join(workspace, 'gopurs_runtime/runtime.go'));
  copyFileSync(join(libraryRoot, 'gopurs-node-event-emitter/src/Node/EventEmitter.go'), join(workspace, 'emitter.go'));
  const source = readFileSync(fileURLToPath(new URL('../src/Node/Stream.go', import.meta.url)), 'utf8');
  writeFileSync(join(workspace, 'stream.go'), source.replace(/^package Node_Stream\b/, 'package Node_EventEmitter'));
  writeFileSync(join(workspace, 'aliases.go'), `package Node_EventEmitter
var Node_EventEmitter_NewImpl = NewImpl
var Node_EventEmitter_GopursUnsafeEmitFn1 = GopursUnsafeEmitFn1
var Node_EventEmitter_GopursUnsafeEmitFn2 = GopursUnsafeEmitFn2
var Node_EventEmitter_GopursUnsafeOn = GopursUnsafeOn
var Node_EventEmitter_GopursUnsafeOff = GopursUnsafeOff
var Node_Stream_WriteImpl = WriteImpl
var Node_Stream_WriteStringImpl = WriteStringImpl
var Node_Stream_EndImpl = EndImpl
`);
  copyFileSync(fileURLToPath(new URL('./pipe-native_test.go', import.meta.url)), join(workspace, 'pipe_test.go'));
  writeFileSync(join(workspace, 'go.mod'), 'module gopurs/output\n\ngo 1.22\n');
  const race = process.env.GOPURS_NATIVE_TEST_RACE === '1' ? ['-race'] : [];
  execFileSync('go', ['test', ...race, '-count=1', '-timeout=15s', '-v', '.'], {
    cwd: workspace, stdio: 'inherit', timeout: 120_000,
    env: { ...process.env, GOWORK: 'off', GOTOOLCHAIN: 'local', GOPROXY: 'off', GOSUMDB: 'off' },
  });
} finally {
  rmSync(workspace, { recursive: true, force: true });
}
