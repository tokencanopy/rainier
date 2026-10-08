#!/usr/bin/env node
import { ensureBinary, runBinary } from './runtime.js';

try {
  const executable = await ensureBinary();
  await runBinary(executable, process.argv.slice(2));
} catch (error) {
  console.error(`rainier: ${error.message}`);
  console.error('Check network access to GitHub and cache-directory permissions. Standalone binaries: https://github.com/tokencanopy/rainier/releases/tag/v0.0.11');
  process.exitCode = 1;
}
