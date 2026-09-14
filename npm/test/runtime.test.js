import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, readFile, rm, chmod } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileSync, spawn } from 'node:child_process';
import { selectRelease, ensureBinary, download } from '../runtime.js';

const hash = value => createHash('sha256').update(value).digest('hex');
async function fixture(t) {
  const dir = await mkdtemp(join(tmpdir(), 'rainier-npm-test-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const source = join(dir, 'source');
  await mkdir(source);
  const binary = '#!/bin/sh\nprintf "%s\\n" "$@"\nexit 7\n';
  await writeFile(join(source, 'rainier'), binary, { mode: 0o755 });
  const archive = join(dir, 'source.tar.gz');
  execFileSync('tar', ['-czf', archive, '-C', source, 'rainier']);
  const bytes = await readFile(archive);
  const release = { version: '0.0.11', platform: 'linux', arch: 'x64', url: 'https://example.test/release.tar.gz', archiveHash: hash(bytes), binaryHash: hash(binary) };
  return { dir, binary, bytes, release, cache: join(dir, 'cache'), download: async () => bytes };
}

test('maps all supported platforms to immutable v0.0.11 assets', () => {
  for (const platform of ['darwin', 'linux']) {
    for (const [arch, goArch] of [['arm64', 'arm64'], ['x64', 'amd64']]) {
      const release = selectRelease(platform, arch);
      assert.equal(release.url, `https://github.com/tokencanopy/rainier/releases/download/v0.0.11/rainier_0.0.11_${platform}_${goArch}.tar.gz`);
      assert.match(release.archiveHash, /^[a-f0-9]{64}$/);
      assert.match(release.binaryHash, /^[a-f0-9]{64}$/);
    }
  }
  assert.throws(() => selectRelease('win32', 'x64'), /Unsupported platform: win32\/x64/);
  assert.throws(() => selectRelease('linux', 'ia32'), /Unsupported platform/);
});

test('installs verified executable, reuses offline, repairs corrupted cache', async t => {
  const f = await fixture(t);
  const options = { release: f.release, cacheRoot: f.cache, download: f.download };
  const executable = await ensureBinary(options);
  assert.equal(await readFile(executable, 'utf8'), f.binary);
  assert.equal(await ensureBinary({ ...options, download: () => { throw Error('offline'); } }), executable);
  await writeFile(executable, 'corrupt');
  await ensureBinary(options);
  assert.equal(await readFile(executable, 'utf8'), f.binary);
});

test('rejects archive and extracted binary checksum mismatches', async t => {
  const f = await fixture(t);
  await assert.rejects(ensureBinary({ release: f.release, cacheRoot: f.cache, download: async () => Buffer.from('bad') }), /Archive checksum mismatch/);
  await assert.rejects(ensureBinary({ release: { ...f.release, binaryHash: '0'.repeat(64) }, cacheRoot: f.cache, download: f.download }), /Binary checksum mismatch/);
});

test('concurrent first runs publish only a complete verified executable', async t => {
  const f = await fixture(t);
  const paths = await Promise.all(Array.from({ length: 6 }, () => ensureBinary({ release: f.release, cacheRoot: f.cache, download: f.download })));
  assert.equal(new Set(paths).size, 1);
  assert.equal(await readFile(paths[0], 'utf8'), f.binary);
});

test('forwards exact arguments without shell evaluation and preserves exit code', async t => {
  const f = await fixture(t);
  const executable = join(f.dir, 'args.js');
  await writeFile(executable, '#!/usr/bin/env node\nprocess.stdout.write(JSON.stringify(process.argv.slice(2))); process.exit(7);\n');
  await chmod(executable, 0o755);
  const script = `import {runBinary} from ${JSON.stringify(new URL('../runtime.js', import.meta.url).href)}; await runBinary(${JSON.stringify(executable)}, ['space here', '$(echo unsafe)', '--flag=value']);`;
  const child = spawn(process.execPath, ['--input-type=module', '-e', script]);
  let stdout = '';
  child.stdout.on('data', chunk => { stdout += chunk; });
  const code = await new Promise(resolve => child.on('close', resolve));
  assert.equal(code, 7);
  assert.deepEqual(JSON.parse(stdout), ['space here', '$(echo unsafe)', '--flag=value']);
});

test('downloader rejects insecure URLs before any network request', async () => {
  await assert.rejects(download('http://example.test/file'), /HTTPS/);
});

test('downloader follows HTTPS redirects and returns the exact bytes', async () => {
  const responses = [new Response(null, { status: 302, headers: { location: 'https://cdn.example.test/asset' } }), new Response('archive')];
  assert.equal((await download('https://example.test/file', { fetchImpl: async () => responses.shift() })).toString(), 'archive');
});

test('downloader bounds redirects and rejects HTTPS downgrades and HTTP errors', async () => {
  const redirect = location => async () => new Response(null, { status: 302, headers: { location } });
  await assert.rejects(download('https://example.test/file', { fetchImpl: redirect('/loop') }), /Too many download redirects/);
  await assert.rejects(download('https://example.test/file', { fetchImpl: redirect('http://example.test/file') }), /HTTPS/);
  await assert.rejects(download('https://example.test/file', { fetchImpl: async () => new Response(null, { status: 404 }) }), /HTTP 404/);
  await assert.rejects(download('https://example.test/file', { fetchImpl: async () => new Response(null, { status: 302 }) }), /no location/);
});

test('downloader bounds advertised and streamed response sizes', async () => {
  await assert.rejects(download('https://example.test/file', { limit: 3, fetchImpl: async () => new Response('large', { headers: { 'content-length': '5' } }) }), /size limit/);
  await assert.rejects(download('https://example.test/file', { limit: 3, fetchImpl: async () => new Response('large') }), /size limit/);
});

test('downloader aborts a stalled request', async () => {
  const fetchImpl = async (_url, { signal }) => new Promise((_resolve, reject) => {
    signal.addEventListener('abort', () => reject(signal.reason), { once: true });
  });
  await assert.rejects(download('https://example.test/file', { fetchImpl, timeoutMs: 10 }), /timed out/);
});

test('forwards termination to the child and preserves signal exit', async t => {
  const f = await fixture(t);
  const script = `import {runBinary} from ${JSON.stringify(new URL('../runtime.js', import.meta.url).href)}; await runBinary(process.execPath, ['-e', 'process.stdout.write("ready"); setInterval(() => {}, 1000)']);`;
  const child = spawn(process.execPath, ['--input-type=module', '-e', script]);
  t.after(() => child.kill('SIGKILL'));
  const exit = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
  await new Promise((resolve, reject) => { child.stdout.once('data', resolve); child.once('error', reject); });
  child.kill('SIGTERM');
  assert.deepEqual(await exit, { code: null, signal: 'SIGTERM' });
});
