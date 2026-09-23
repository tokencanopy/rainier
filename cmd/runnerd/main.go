// cmd/runnerd/main.go
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tokencanopy/rainier/checkpoint"
	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/runnerd"
)

func main() {
	listen := flag.String("listen", "0.0.0.0:8080", "control + relay listen address")
	dialBase := flag.String("dial-base", "ws://runnerd:8080", "URL sessiond containers dial to register")
	image := flag.String("image", "rainier-session:latest", "default session image")
	network := flag.String("network", "rainier-internal", "internal docker network for sessions")
	egressAdmin := flag.String("egress-admin", "http://egressd:3129", "egressd admin URL")
	slots := flag.Int("slots", 16, "how many sandboxes this runner may hold at once, running or warm-suspended; a cold-stopped session keeps its files but no slot")
	idleStop := flag.Duration("idle-stop", 30*time.Minute,
		"stop a session whose agent process has exited and that has had no attachment for this `duration`, exactly as rainier stop does: the container stops, the files are kept, the slot comes back, and an attach resumes it. A session whose agent is still running is never stopped, however long nobody has watched it. 0 disables it")
	controld := flag.String("controld", "", "controld URL to dial (ws://host:port); enables agent (dial) mode when set")
	runnerToken := flag.String("runner-token", envDefault("RAINIER_RUNNER_TOKEN", ""),
		"bearer token for the controld dial (required when --controld is set; or set RAINIER_RUNNER_TOKEN, which keeps it out of the process list)")
	driverFlag := flag.String("driver", envDefault("RAINIER_RUNNER_DRIVER", "docker"),
		"execution driver for session sandboxes: docker | microvm")
	kernelPath := flag.String("kernel", envDefault("RAINIER_KERNEL_PATH", ""),
		"guest vmlinux kernel path for the microvm driver (required when --driver=microvm; the runner refuses to start without a readable one)")
	rootfsPath := flag.String("rootfs", envDefault("RAINIER_ROOTFS_PATH", ""),
		"base ext4 rootfs image path for the microvm driver (required when --driver=microvm; the runner refuses to start without a readable one)")
	microvmStateDir := flag.String("microvm-state-dir", envDefault("RAINIER_MICROVM_STATE_DIR", ""),
		"host directory holding microVM instance records, sockets, and every session's workspace and agent-home disk image (required when --driver=microvm; there is deliberately no temp-directory default, which would put a tenant's files somewhere the host reaps)")
	microvmImageDir := flag.String("microvm-image-dir", envDefault("RAINIER_MICROVM_IMAGE_DIR", ""),
		"directory this host fetches environment images from: an index.json mapping refs to digests, and one `<digest>.ext4` file per image (ADR-0003 §2.7 item 3). Mutually exclusive with --microvm-image-url; with neither, the runner serves only the images already in its store and reports a clear error for anything else")
	microvmImageURL := flag.String("microvm-image-url", envDefault("RAINIER_MICROVM_IMAGE_URL", ""),
		"HTTPS base URL this host fetches environment images from: `<base>/index.json` and `<base>/<digest>.ext4`. Every image is verified against its digest while it streams, so the transport is not trusted for the bytes — only for which digest a ref means. No OCI image is ever unpacked on a microVM host")
	microvmVCPUs := flag.Int("microvm-vcpus", envIntDefault("RAINIER_MICROVM_VCPUS", 4),
		"vCPUs per microVM session (ADR-0003 §5.1 per-session floor: 4)")
	microvmMemoryMiB := flag.Int("microvm-memory-mib", envIntDefault("RAINIER_MICROVM_MEMORY_MIB", 8192),
		"memory in MiB per microVM session (ADR-0003 §5.1 per-session floor: 8192)")
	microvmGuestCIDR := flag.String("microvm-guest-cidr", envDefault("RAINIER_MICROVM_GUEST_CIDR", ""),
		"host-local range the guest link addresses are carved from, one /30 per session slot (ADR-0003 §5.2); empty means 10.201.0.0/16")
	microvmUplinkCIDR := flag.String("microvm-uplink-cidr", envDefault("RAINIER_MICROVM_UPLINK_CIDR", ""),
		"host-local range the per-slot veth pair addresses are carved from, one /30 per slot; empty means 10.202.0.0/16. It must not overlap --microvm-guest-cidr")
	microvmSlotPrefix := flag.String("microvm-slot-prefix", envDefault("RAINIER_MICROVM_SLOT_PREFIX", ""),
		"prefix for the network namespace, veth and TAP names this runner creates, and what it recognises as its own when reclaiming a previous run's leftovers; empty means rnr")
	microvmNetnsDir := flag.String("microvm-netns-dir", envDefault("RAINIER_MICROVM_NETNS_DIR", ""),
		"directory holding named network namespaces; empty means /var/run/netns, which is where ip netns puts them")
	microvmEgressProxy := flag.String("microvm-egress-proxy", envDefault("RAINIER_MICROVM_EGRESS_PROXY", ""),
		"`ip:port` of the egress proxy, and the only host-side destination a microVM guest's firewall allows (ADR-0003 §4.3). Defaults to the host and port of --proxy-url when that is already an IP literal; a --proxy-url naming a host must be given here as an address, because a rule that named a host would be a rule a guest could move by answering a DNS query")
	microvmJailer := flag.String("microvm-jailer", envDefault("RAINIER_MICROVM_JAILER", ""),
		"path to Firecracker's jailer; empty means `jailer` on PATH. Every microVM runs under it (ADR-0003 §4.5: per-VM uid and gid, its own cgroup, its own netns, a chroot, seccomp) and the runner refuses to start without it")
	microvmUIDFirst := flag.Int("microvm-uid-first", envIntDefault("RAINIER_MICROVM_UID_FIRST", 0),
		"bottom of the per-VM uid and gid range the jailer drops each microVM to; 0 means 200000. A VM's uid is this plus its network slot index, so it is the same number across a runnerd restart and two live sessions can never share one")
	microvmUIDCount := flag.Int("microvm-uid-count", envIntDefault("RAINIER_MICROVM_UID_COUNT", 0),
		"size of the per-VM uid and gid range; 0 means 4096. It must be larger than --slots, since a VM's uid is its slot index above --microvm-uid-first, and the runner refuses to start otherwise")
	microvmCgroupParent := flag.String("microvm-cgroup-parent", envDefault("RAINIER_MICROVM_CGROUP_PARENT", ""),
		"cgroup v2 parent the jailer creates each microVM's cgroup under, and where host-side metering reads cpu.stat and memory.current (ADR-0003 §4.6); empty means rainier")
	microvmCgroupRoot := flag.String("microvm-cgroup-root", envDefault("RAINIER_MICROVM_CGROUP_ROOT", ""),
		"cgroup v2 mount point; empty means /sys/fs/cgroup. It is where host-side metering reads each microVM's cpu.stat and memory.current from")
	microvmSeccompOff := flag.Bool("microvm-seccomp-off", os.Getenv("RAINIER_MICROVM_SECCOMP_OFF") == "1",
		"turn OFF the jailer's seccomp filter on the Firecracker process. Seccomp is on by default and this exists for diagnosing a filter rejection on a new kernel, not for production")
	checkpointStoreDir := flag.String("checkpoint-store-dir", envDefault("RAINIER_CHECKPOINT_STORE_DIR", ""),
		"host directory holding the portable workspace checkpoints a cold suspend writes (required when --driver=microvm). A cold suspend does not report success until a session's workspace is a committed, verified checkpoint in here (ADR-0003 §4.4), and a deep-dormant resume rebuilds the workspace image from it. A hosted cell puts these in regional object storage instead")
	checkpointKeyFile := flag.String("checkpoint-key-file", envDefault("RAINIER_CHECKPOINT_KEY_FILE", ""),
		"file holding the 32-byte key every workspace checkpoint is wrapped under, as 64 hex characters or 32 raw bytes, mode 0600 (required when --driver=microvm). Without it the checkpoints in --checkpoint-store-dir would be a tenant's files in the clear on this host; with it, losing this file means losing every checkpoint it wrapped")
	checkpointRestoreUID := flag.Int("checkpoint-restore-uid", envIntDefault("RAINIER_CHECKPOINT_RESTORE_UID", 1000),
		"uid a restored workspace's files are given to before they become a filesystem: the user the session image runs its agent as. A checkpoint records modes and not owners, so without this a restored workspace comes back owned by root and the agent cannot write it. 0 means leave every file owned by this runner")
	checkpointRestoreGID := flag.Int("checkpoint-restore-gid", envIntDefault("RAINIER_CHECKPOINT_RESTORE_GID", 1000),
		"gid a restored workspace's files are given to; see --checkpoint-restore-uid")
	var microvmControlPlane capabilityFlag
	flag.Var(&microvmControlPlane, "microvm-control-plane-cidr",
		"a regional control-plane range a microVM guest must not be able to reach; repeatable, or set RAINIER_MICROVM_CONTROL_PLANE_CIDRS to a comma-separated list")
	hostname, _ := os.Hostname()
	runnerName := flag.String("runner-name", hostname, "name this runner announces to controld")
	proxyURL := flag.String("proxy-url", "", "egress proxy URL injected into every session (forwarded to controld dial mode)")
	seccompProfile := flag.String("session-seccomp-profile", "", "host path to an optional seccomp profile for session containers")
	appArmorProfile := flag.String("session-apparmor-profile", "", "name of an optional loaded AppArmor profile for session containers")
	var capabilities capabilityFlag
	flag.Var(&capabilities, "capability",
		"a portable capability this runner claims (e.g. gpu); repeatable, or set RAINIER_RUNNER_CAPABILITIES to a comma-separated list")
	flag.Parse()
	// The flag wins whole: an operator who passed --capability is naming the
	// complete set, so the environment's list is a DEFAULT, not something the
	// flag adds to. Same rule as every other flag here.
	if len(capabilities) == 0 {
		capabilities = splitCapabilities(os.Getenv("RAINIER_RUNNER_CAPABILITIES"))
	}
	if len(microvmControlPlane) == 0 {
		microvmControlPlane = splitCapabilities(os.Getenv("RAINIER_MICROVM_CONTROL_PLANE_CIDRS"))
	}

	var drv driver.Driver
	switch *driverFlag {
	case "docker":
		drv = driver.NewDocker(driver.DockerOpts{
			Image:                  *image,
			Network:                *network,
			TotalSlots:             *slots,
			SessionSeccompProfile:  *seccompProfile,
			SessionAppArmorProfile: *appArmorProfile,
		})
	case "microvm":
		// The microVM driver refuses to construct itself on a host that
		// cannot boot a microVM — no /dev/kvm, no kernel, no rootfs, no
		// mkfs.ext4, no firecracker — and this is a Fatal rather than a
		// fallback for the same reason: a runner that started anyway would
		// register with controld, accept placements, and report every session
		// running while nothing executed.
		// The one host-side destination the guest firewall lets through. A
		// runner that was given a proxy it cannot name as an address is a
		// runner whose sessions would have a firewall with no way out, so
		// this is a Fatal and not a warning.
		proxyAddr, proxyPort, err := egressProxyEndpoint(*microvmEgressProxy, *proxyURL)
		if err != nil {
			log.Fatalf("--driver=microvm: %v", err)
		}
		// Where environment images come from. Two sources would be two
		// answers to "which bytes is this ref", so naming both is a
		// configuration mistake rather than a preference order.
		var imageSource driver.ImageSource
		switch {
		case *microvmImageDir != "" && *microvmImageURL != "":
			log.Fatal("--driver=microvm: --microvm-image-dir and --microvm-image-url both name where environment images come from; give one")
		case *microvmImageDir != "":
			imageSource = driver.DirImageSource{Dir: *microvmImageDir}
		case *microvmImageURL != "":
			imageSource = driver.HTTPImageSource{Base: *microvmImageURL}
		}
		// Where a cold suspend's workspace checkpoint goes, and what wraps it.
		// Both are REQUIRED here, and this is a Fatal for the same reason the
		// kernel and the rootfs are: a runner that started without them would
		// accept placements and then fail every cold suspend — with a tenant's
		// work still inside a VM the durability barrier will not let it
		// terminate.
		ckpt, err := checkpointConfig(*checkpointStoreDir, *checkpointKeyFile,
			*checkpointRestoreUID, *checkpointRestoreGID)
		if err != nil {
			log.Fatalf("--driver=microvm: %v", err)
		}
		mvm, err := driver.NewMicrovm(driver.MicrovmOpts{
			Checkpoint:  ckpt,
			KernelPath:  *kernelPath,
			BaseRootfs:  *rootfsPath,
			StateDir:    *microvmStateDir,
			TotalSlots:  *slots,
			VCPU:        *microvmVCPUs,
			MemoryMiB:   *microvmMemoryMiB,
			ImageSource: imageSource,

			SlotGuestCIDR:  *microvmGuestCIDR,
			SlotUplinkCIDR: *microvmUplinkCIDR,
			SlotNamePrefix: *microvmSlotPrefix,
			NetnsDir:       *microvmNetnsDir,

			EgressProxyAddr:   proxyAddr,
			EgressProxyPort:   proxyPort,
			ControlPlaneCIDRs: microvmControlPlane,

			CgroupRoot: *microvmCgroupRoot,
			Jail: driver.JailOpts{
				JailerPath:   *microvmJailer,
				UIDFirst:     *microvmUIDFirst,
				UIDCount:     *microvmUIDCount,
				CgroupParent: *microvmCgroupParent,
				SeccompOff:   *microvmSeccompOff,
			},
		})
		if err != nil {
			log.Fatalf("--driver=microvm: %v", err)
		}
		drv = mvm
	default:
		log.Fatalf("unknown --driver %q (valid: docker, microvm)", *driverFlag)
	}
	// *proxyURL reaches New directly now (Task 13) so the local HTTP-only
	// surface — today's default, and every dev/CI compose run — injects it
	// into every driver.Spec too, not just agent (dial) mode below.
	s := runnerd.New(drv, *dialBase, *egressAdmin, *proxyURL)

	// Rebuild the registry from the driver's labeled containers before
	// serving, so a restart is truthful about sessions that outlived it
	// instead of forgetting them outright. Always runs first, in both HTTP
	// and dial mode.
	if err := s.Recover(context.Background()); err != nil {
		log.Fatalf("recover: %v", err)
	}

	// Started after Recover so the sweep cannot race its registry writes, and
	// before either serving mode takes the foreground.
	//
	// Recovered sessions are never auto-stopped until they report a child
	// exit — the exit that would make them candidates lived only in the memory
	// of the process that just died. That is the safe direction and it is also
	// a real gap: a runner restarted onto a box full of finished sessions
	// reclaims none of them until each is removed by hand. A durable activity
	// record is #85's job.
	go s.RunIdleStop(context.Background(), *idleStop)

	log.Printf("runnerd on %s (dial-base %s)", *listen, *dialBase)
	if *controld == "" {
		// HTTP-only mode (today's default): the local surface is the whole
		// program, so it owns the foreground.
		log.Fatal(http.ListenAndServe(*listen, s.Handler()))
	}

	// Fail closed: dialing controld with no bearer token would let anyone
	// impersonate this runner on any network path that can reach controld.
	if *runnerToken == "" {
		log.Fatal("--runner-token is required when --controld is set")
	}
	// The local HTTP surface stays up as the dev/debug path, unchanged, but
	// now runs in the background — the agent dial owns the foreground.
	go func() {
		log.Fatal(http.ListenAndServe(*listen, s.Handler()))
	}()
	log.Printf("dialing controld at %s as %q", *controld, *runnerName)
	if err := s.RunAgent(context.Background(), runnerd.AgentConfig{
		ControldURL:  *controld,
		Token:        *runnerToken,
		RunnerName:   *runnerName,
		ProxyURL:     *proxyURL,
		Capabilities: capabilities,
	}); err != nil {
		log.Fatalf("agent: %v", err)
	}
}

// checkpointConfig builds the microVM driver's checkpoint configuration from
// the two required flags, or says which one is missing and why it matters.
//
// It is a function of its arguments rather than of the flag variables so that
// the refusals are testable: every one of them is a condition a real deployment
// gets wrong at some point, and "the runner started anyway" is the outcome none
// of them may have.
func checkpointConfig(storeDir, keyFile string, uid, gid int) (*driver.CheckpointOpts, error) {
	switch {
	case storeDir == "":
		return nil, errors.New("--checkpoint-store-dir is required: a cold suspend turns a session's workspace into a portable checkpoint before it terminates the VM (ADR-0003 §4.4), and there is deliberately no default — a temp-directory one would put a tenant's only durable copy somewhere the host reaps")
	case keyFile == "":
		return nil, errors.New("--checkpoint-key-file is required: a workspace checkpoint is encrypted under it, and an unencrypted one would be a tenant's files in the clear on this host")
	case uid < 0 || gid < 0:
		return nil, errors.New("--checkpoint-restore-uid and --checkpoint-restore-gid must not be negative")
	}
	store, err := driver.NewDirBlobStore(storeDir)
	if err != nil {
		return nil, err
	}
	key, err := driver.LoadCheckpointKey(keyFile)
	if err != nil {
		return nil, err
	}
	keys, err := checkpoint.NewStaticKeyWrapper(driver.SelfHostedCheckpointKeyRef, key)
	if err != nil {
		return nil, err
	}
	return &driver.CheckpointOpts{
		Store:    store,
		Keys:     keys,
		KeyRef:   driver.SelfHostedCheckpointKeyRef,
		OwnerUID: uid,
		OwnerGID: gid,
	}, nil
}

// egressProxyEndpoint resolves the one host-side destination a microVM
// guest's firewall allows, from --microvm-egress-proxy or, failing that, from
// --proxy-url.
//
// It insists on an IP literal. The firewall rule is the boundary between a
// tenant's guest and this host's own network; a rule that named a host would
// be a rule the guest could move by answering a DNS query, and DNS is one of
// the few things a sandboxed workload can usually still influence.
//
// Three answers, and no fourth:
//   - an explicit address:port, used as given;
//   - no proxy at all, which is a legal runner and gets a firewall with no
//     exception rather than a firewall that is switched off;
//   - a proxy that cannot be named as an address, which is an error.
func egressProxyEndpoint(explicit, proxyURL string) (string, int, error) {
	raw := explicit
	if raw == "" {
		if proxyURL == "" {
			return "", 0, nil
		}
		u, err := url.Parse(proxyURL)
		if err != nil {
			return "", 0, fmt.Errorf("--proxy-url %q is not a URL: %w", proxyURL, err)
		}
		raw = u.Host
		if u.Port() == "" {
			switch u.Scheme {
			case "https":
				raw = net.JoinHostPort(u.Hostname(), "443")
			default:
				raw = net.JoinHostPort(u.Hostname(), "80")
			}
		}
	}

	host, portStr, err := net.SplitHostPort(raw)
	if err != nil {
		return "", 0, fmt.Errorf("the egress proxy endpoint %q is not ip:port; pass --microvm-egress-proxy: %w", raw, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return "", 0, fmt.Errorf("the egress proxy %q names a host and not an address. The per-slot firewall allows exactly one host-side destination (ADR-0003 §4.3) and it must be an IP: a rule naming a host is a rule the guest can move by answering a DNS query. Pass --microvm-egress-proxy=ip:port", host)
	}
	// Refused here rather than three layers down, where the message would be
	// about a rendered ruleset: the guest link is IPv4 and the per-slot
	// firewall drops IPv6 from the guest outright, so a v6 proxy is a proxy
	// no session could reach.
	if ip.To4() == nil {
		return "", 0, fmt.Errorf("the egress proxy %q is IPv6. A microVM guest's link is IPv4 and its firewall drops IPv6 from the guest outright, so no session could reach it; give the proxy's IPv4 address", host)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return "", 0, fmt.Errorf("the egress proxy port %q is not a port", portStr)
	}
	return host, port, nil
}

// capabilityFlag collects a repeatable --capability into the list runnerd
// announces, in the order they were passed. Nothing is validated here: a
// capability is a claim about this runner, and controld is the one that
// decides whether it will accept and schedule on it — refusing a token
// locally would only move the same error to a place the operator cannot see
// the fleet's answer.
type capabilityFlag []string

func (c *capabilityFlag) String() string { return strings.Join(*c, ",") }

func (c *capabilityFlag) Set(v string) error {
	*c = append(*c, v)
	return nil
}

// splitCapabilities reads RAINIER_RUNNER_CAPABILITIES: a comma-separated
// list, trimmed, with empty entries dropped so a trailing comma (or an unset
// variable) announces nothing rather than a nameless capability.
func splitCapabilities(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// envDefault returns env's value when it is set and non-empty, else def — the
// same rule cmd/controld applies to every one of its flags, so a secret can be
// handed to runnerd through the environment instead of the command line,
// where every user on the host could read it with ps.
func envDefault(env, def string) string {
	if v := os.Getenv(env); v != "" {
		return v
	}
	return def
}

// envIntDefault is envDefault for an integer flag. An unparseable value is
// the operator's mistake, not a reason to silently run on the default: a
// microVM slot sized from a typo would make this host's capacity describe a
// machine nobody configured.
func envIntDefault(env string, def int) int {
	v := os.Getenv(env)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("%s=%q is not an integer", env, v)
	}
	return n
}
