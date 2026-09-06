import { createHash } from 'node:crypto';
import { chmod, lstat, mkdir, mkdtemp, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { isAbsolute, join } from 'node:path';
import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import { assets, version } from './release.js';

const exec = promisify(execFile);
const maxBytes = 16 * 1024 * 1024;
const digest = bytes => createHash('sha256').update(bytes).digest('hex');

export function selectRelease(platform = process.platform, arch = process.arch) {
  const asset = assets[`${platform}_${arch}`];
  if (!asset) throw new Error(`Unsupported platform: ${platform}/${arch}. Rainier supports macOS and Linux on arm64 and x64.`);
  const [goArch, archiveHash, binaryHash] = asset;
  return { version, platform, arch, archiveHash, binaryHash,
    url: `https://github.com/tokencanopy/rainier/releases/download/v${version}/rainier_${version}_${platform}_${goArch}.tar.gz` };
}

export async function download(url, { fetchImpl = fetch, timeoutMs = 60_000, limit = maxBytes } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('Download timed out')), timeoutMs);
  try {
    for (let redirects = 0; redirects <= 5; redirects++) {
      if (new URL(url).protocol !== 'https:') throw new Error('Download requires HTTPS');
      const response = await fetchImpl(url, { redirect: 'manual', signal: controller.signal });
      if ([301, 302, 303, 307, 308].includes(response.status)) {
        await response.body?.cancel();
        const location = response.headers.get('location');
        if (!location) throw new Error('Download redirect has no location');
        url = new URL(location, url).href;
        continue;
      }
      if (!response.ok) {
        await response.body?.cancel();
        throw new Error(`Download failed: HTTP ${response.status}`);
      }
      if (Number(response.headers.get('content-length')) > limit) {
        await response.body?.cancel();
        throw new Error('Download exceeds size limit');
      }
      const chunks = [];
      let size = 0;
      if (!response.body) throw new Error('Download has no body');
      for await (const chunk of response.body) {
        size += chunk.length;
        if (size > limit) throw new Error('Download exceeds size limit');
        chunks.push(chunk);
      }
      return Buffer.concat(chunks);
    }
    throw new Error('Too many download redirects');
  } finally {
    clearTimeout(timer);
  }
}

function defaultCacheRoot() {
  const xdg = process.env.XDG_CACHE_HOME;
  return join(xdg && isAbsolute(xdg) ? xdg : join(homedir(), '.cache'), 'rainier');
}

async function verifiedFile(path, expectedHash) {
  try {
    const stat = await lstat(path);
    return stat.isFile() && stat.size <= maxBytes && digest(await readFile(path)) === expectedHash;
  } catch (error) {
    if (error.code === 'ENOENT') return false;
    throw error;
  }
}

export async function ensureBinary({ release = selectRelease(), cacheRoot = defaultCacheRoot(), download: fetchArchive = download } = {}) {
  const dir = join(cacheRoot, `${release.version}-${release.platform}-${release.arch}`);
  const executable = join(dir, 'rainier');
  await mkdir(dir, { recursive: true, mode: 0o700 });
  if (await verifiedFile(executable, release.binaryHash)) {
    await chmod(executable, 0o755);
    return executable;
  }
  const stage = await mkdtemp(join(dir, '.install-'));
  try {
    const bytes = await fetchArchive(release.url);
    if (digest(bytes) !== release.archiveHash) throw new Error('Archive checksum mismatch; refusing to execute downloaded code');
    const archive = join(stage, 'release.tar.gz');
    await writeFile(archive, bytes, { mode: 0o600 });
    await exec('tar', ['-xzf', archive, '-C', stage, 'rainier'], { timeout: 10_000 });
    const binary = join(stage, 'rainier');
    if (!await verifiedFile(binary, release.binaryHash)) throw new Error('Binary checksum mismatch; refusing to execute downloaded code');
    await chmod(binary, 0o755);
    // Each writer publishes the same verified bytes; rename never exposes a partial file.
    await rename(binary, executable);
    return executable;
  } finally {
    await rm(stage, { recursive: true, force: true });
  }
}

export async function runBinary(executable, args) {
  const child = spawn(executable, args, { stdio: 'inherit' });
  const signals = ['SIGINT', 'SIGTERM', 'SIGHUP'];
  const handlers = signals.map(signal => {
    const handler = () => child.kill(signal);
    process.on(signal, handler);
    return handler;
  });
  let result;
  try {
    result = await new Promise((resolve, reject) => {
      child.once('error', reject);
      child.once('exit', (code, signal) => resolve({ code, signal }));
    });
  } finally {
    signals.forEach((signal, i) => process.removeListener(signal, handlers[i]));
  }
  if (result.signal) process.kill(process.pid, result.signal);
  else process.exitCode = result.code ?? 1;
}
