# Rainier CLI beta

Persistent remote coding-agent sessions from your terminal. This package runs
the native [Rainier](https://github.com/tokencanopy/rainier) CLI; it does not
install or provision a control plane or runner.

## Run without a global installation

```sh
npx @tokencanopy/rainier@beta --help
```

Or pin this release: `npx @tokencanopy/rainier@0.0.10 --help`.

## Install for daily use

```sh
npm install -g @tokencanopy/rainier@beta
rainier version
rainier --help
```

Requires Node.js 22 or newer, `tar`, and macOS or Linux on Apple Silicon/ARM64
or Intel/AMD64. Windows is not supported by this release.

The first invocation downloads the matching native v0.0.10 binary from GitHub
over HTTPS and verifies pinned SHA-256 hashes for both the archive and binary.
There are no npm dependencies or installation scripts. Subsequent invocations
verify and reuse the cached binary; they do not download it again unless it is
missing or corrupted. `npx` itself may still contact the npm registry.

The cache is `$XDG_CACHE_HOME/rainier` when `XDG_CACHE_HOME` is an absolute path,
otherwise `~/.cache/rainier`. It must be writable by your user. First use needs
network access to GitHub and its release-asset CDN. The downloader does not
configure an HTTP proxy; use a standalone download if your network requires one.
No Go compiler is needed. Without Node.js, use a
[standalone release binary](https://github.com/tokencanopy/rainier/releases/tag/v0.0.10).
The macOS release is not Developer ID signed or notarized.

## Connect to Rainier

You need access to an existing compatible Rainier deployment. For self-hosting,
see the [deployment guide](https://github.com/tokencanopy/rainier/blob/main/docs/deploy-gce.md).

```sh
rainier login --from-gh --server https://rainier.example.test
rainier doctor
rainier ls
```

For hosted pilot access, use the cloud endpoint supplied with your invitation:

```sh
rainier login --cloud https://cloud.example.test
rainier doctor
```

Follow the [first-session guide](https://github.com/tokencanopy/rainier/blob/v0.0.10/docs/cli-quickstart.md)
to select an environment, sign in to Claude Code or Codex, and start a session.
`rainier doctor` checks basic readiness; it does not install agents or prove that
provider credentials work. Agent credential support requires compatible server components.

Publishing this CLI does not imply hosted signup is generally available.
Version 0.0.10 is beta: interfaces may change. npm's `beta` tag selects the beta
package; the package always downloads its own pinned binary, never a moving
GitHub release. To update, rerun the install command above with the desired tag
or version. Run `rainier version` to confirm `rainier v0.0.10`. There is no native CLI auto-updater.

## Maintainers

From the repository root, run `make verify` and `npm --prefix npm test`. Smoke-test an
`npm pack ./npm` tarball through both `npx --package <tarball> rainier --help`
and a global installation with a temporary prefix/cache. Publish the verified
tarball using `npm publish <tarball> --access public --tag beta`.
Never replace v0.0.10 assets or mutate their pinned hashes after publication;
new binaries require a new release and package version.
