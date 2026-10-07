# Experimental full-VM artifact bundle

This operator-only tool reuses `checkpoint` streaming authenticated encryption
for a **disposable same-host memory-resume experiment**. It changes no driver,
resume API, checkpoint format, or production policy. Authenticated guest RAM can
contain credentials; production's existing prohibition remains in force.

Build with `GOOS=linux GOARCH=amd64 go build -o memorybundle ./experiments/memorybundle`.
The source is a private (`0700`) directory containing exactly six nonempty regular
files: `memory`, `vmstate`, `rootfs`, `workspace`, `home`, and `metadata.json`.
The operator must pause the guest and capture matching disk contents before
sealing; this tool cannot establish that consistency itself.

```sh
memorybundle -action seal -store /private/experiment/bundle -key /private/experiment/key -dir /private/experiment/capture -generation 1
memorybundle -action verify -store /private/experiment/bundle -key /private/experiment/key -generation 1
memorybundle -action open -store /private/experiment/bundle -key /private/experiment/key -dir /private/experiment/restored -generation 1
```

Generate a random 32-byte key in a separate `0600` file. Keep the source, store,
key, and destination parent operator-owned and inaccessible to other writers.
Use a dedicated store, never a production checkpoint store: the context is
intentionally fixed to `memory-experiment` / `synthetic-session`. Specify a new
positive generation for each capture. There is no control-plane authorization
or freshness ledger; possession of the local key authorizes this experiment.

`seal` verifies the encrypted stream after writing it. `verify` authenticates
content but does not check the six-file layout. `open` authenticates into a private
staging directory, validates the layout, then publishes a new destination. It
refuses existing destinations and serializes cooperating opens with an exclusive
lock file. An interrupted open may leave private staging/lock files; inspect and
remove them only after confirming no operation remains active.

Before starting any restored VMM, the separate operator harness must validate
software/CPU compatibility, pin every disk path, retain the network slot, confirm
the original VMM is gone, and prevent replay of an already consumed generation.
The tool does not start or stop VMs, interpret metadata, refresh credentials,
compress sparse files, or preserve sparse allocation on extraction. Full logical
memory and disk sizes therefore determine ciphertext and restore storage costs.

Delete **all** encrypted and plaintext copies, staging files, keys, and disposable
host disks after the experiment. Removing a credential from the live guest does
not remove its earlier copy from a snapshot. Never upload bundles or guest logs
to a PR, CI artifact store, or public issue.
