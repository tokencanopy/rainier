// internal/driver/microvm_kvm_script_test.go
//
// scripts/microvm-kvm-test.sh, driven against fakes.
//
// The harness it launches only runs on a real KVM host, which is exactly why
// the script around it has to be tested somewhere that is not one: it is the
// piece a Phase A operator runs first, and its failure mode — a run that
// reports success having skipped, or a precondition check that names one
// missing thing and hides four others — is not something the harness can
// catch, because the harness never ran.
//
// So every binary it looks for is a shell stub on a fabricated PATH, the KVM
// device is a regular file, and `go` is a stub that writes the timings file
// the real harness would. What is under test is the script's own decisions:
// which preconditions it reports, how it invokes the harness, what it calls a
// pass, and what it writes down.
package driver

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// kvmScriptPath is the script under test.
func kvmScriptPath(t *testing.T) string {
	t.Helper()
	p, err := filepath.Abs("../../scripts/microvm-kvm-test.sh")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// kvmScriptHost is a fabricated machine: a PATH of stubs, an images directory,
// a KVM "device", and somewhere for the evidence to land.
type kvmScriptHost struct {
	dir    string
	binDir string
	images string
	outDir string
	kvm    string
	// goStub is the body of the fake `go`, so a test can make the harness
	// pass, fail, or skip without one existing.
	goStub string
}

// newKVMScriptHost builds a machine on which every precondition holds.
func newKVMScriptHost(t *testing.T) *kvmScriptHost {
	t.Helper()
	dir := t.TempDir()
	h := &kvmScriptHost{
		dir:    dir,
		binDir: filepath.Join(dir, "bin"),
		images: filepath.Join(dir, "images"),
		outDir: filepath.Join(dir, "out"),
		kvm:    filepath.Join(dir, "kvm"),
	}
	for _, d := range []string{h.binDir, h.images} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// A writable regular file stands in for /dev/kvm: the script's check is
	// "readable and writable by this user", which is the check the driver
	// makes, and a regular file answers it the same way.
	if err := os.WriteFile(h.kvm, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"vmlinux", "rootfs.ext4"} {
		if err := os.WriteFile(filepath.Join(h.images, name), []byte("not a real "+name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"firecracker", "jailer", "ip", "nft", "sysctl", "mkfs.ext4"} {
		h.stub(t, name, "#!/bin/sh\necho \""+name+" 1.0.0-stub\"\n")
	}
	h.goStub = "#!/bin/sh\n" +
		// `go version` is the script asking what it is about to run with, and
		// it must not be mistaken for the harness invocation the assertions
		// read back.
		"if [ \"$1\" = version ]; then echo 'go version go0.0.0-stub'; exit 0; fi\n" +
		"printf '%s\\n' \"$@\" > \"$KVM_SCRIPT_ARGS\"\n" +
		"env | grep '^RAINIER_MICROVM' | sort > \"$KVM_SCRIPT_ENV\"\n" +
		"printf '{\"schema\":\"rainier.microvm.kvm-test/v1\",\"boot_to_connected_ms\":1234}\\n' > \"$RAINIER_MICROVM_TEST_TIMINGS\"\n" +
		"echo '--- PASS: TestMicrovmBootSmokeOnKVM (1.23s)'\n" +
		"echo ok\n"
	return h
}

func (h *kvmScriptHost) stub(t *testing.T, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(h.binDir, name), []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
}

// remove takes one stub off the fabricated PATH, standing in for a host that
// does not have that binary.
func (h *kvmScriptHost) remove(t *testing.T, name string) {
	t.Helper()
	if err := os.Remove(filepath.Join(h.binDir, name)); err != nil {
		t.Fatal(err)
	}
}

// run executes the script on this machine and returns its output and status.
func (h *kvmScriptHost) run(t *testing.T) (out string, code int) {
	t.Helper()
	// `go` is written per run so a test can change what the harness "did"
	// between runs.
	h.stub(t, "go", h.goStub)

	cmd := exec.Command("/bin/bash", kvmScriptPath(t))
	cmd.Env = append(os.Environ(),
		// A PATH of stubs plus the real shell utilities the script uses
		// (awk, grep, cut, tr, date, uname). Those are the machine's, because
		// faking coreutils would be testing bash rather than this script.
		"PATH="+h.binDir+string(os.PathListSeparator)+"/usr/bin"+string(os.PathListSeparator)+"/bin",
		"RAINIER_MICROVM_TEST_IMAGES="+h.images,
		"RAINIER_MICROVM_KVM_DEVICE="+h.kvm,
		"RAINIER_MICROVM_KVM_OUT="+h.outDir,
		"KVM_SCRIPT_ARGS="+filepath.Join(h.dir, "go-args"),
		"KVM_SCRIPT_ENV="+filepath.Join(h.dir, "go-env"),
	)
	b, err := cmd.CombinedOutput()
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("running %s: %v\n%s", kvmScriptPath(t), err, b)
		}
		code = exitErr.ExitCode()
	}
	return string(b), code
}

func (h *kvmScriptHost) readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// TestMicrovmKVMScriptRunsTheHarnessAndWritesEvidence. The happy path, and the
// three things about it an operator depends on: the harness is invoked with
// the timeout and the verbosity the runbook cites, it is handed the
// environment the harness reads, and the timings it wrote come back nested in
// one evidence file rather than left for somebody to correlate.
func TestMicrovmKVMScriptRunsTheHarnessAndWritesEvidence(t *testing.T) {
	h := newKVMScriptHost(t)
	out, code := h.run(t)
	if code != 0 {
		t.Fatalf("script exited %d on a host where every precondition holds:\n%s", code, out)
	}

	args := h.readFile(t, filepath.Join(h.dir, "go-args"))
	for _, want := range []string{"test", "./internal/driver", "-v", "-count=1", "-timeout", "30m"} {
		if !strings.Contains(args, want+"\n") {
			t.Fatalf("the harness was invoked without %q:\n%s", want, args)
		}
	}
	// Both tests, named, so the command a runbook quotes says which tests its
	// evidence is for. What catches a pattern that has drifted off them is
	// TestMicrovmKVMScriptRefusesToCallAnEmptyRunAPass, below.
	if !strings.Contains(args, "TestMicrovmBootSmokeOnKVM") || !strings.Contains(args, "TestMicrovmSatisfiesContractOnKVM") {
		t.Fatalf("the -run pattern does not name both harness tests:\n%s", args)
	}

	env := h.readFile(t, filepath.Join(h.dir, "go-env"))
	for _, want := range []string{
		"RAINIER_MICROVM_KVM_TEST=1",
		"RAINIER_MICROVM_TEST_IMAGES=" + h.images,
		"RAINIER_MICROVM_TEST_TIMINGS=" + filepath.Join(h.outDir, "timings.json"),
	} {
		if !strings.Contains(env, want+"\n") {
			t.Fatalf("the harness was not handed %q:\n%s", want, env)
		}
	}

	var evidence struct {
		Schema     string `json:"schema"`
		Result     string `json:"result"`
		OSSCommit  string `json:"oss_commit"`
		RecordedAt string `json:"recorded_at"`
		Artifacts  struct {
			KernelSHA256 string `json:"kernel_sha256"`
			RootfsSHA256 string `json:"rootfs_sha256"`
			RootfsBytes  int64  `json:"rootfs_bytes"`
		} `json:"artifacts"`
		Timings struct {
			Schema            string `json:"schema"`
			BootToConnectedMS int64  `json:"boot_to_connected_ms"`
		} `json:"timings"`
	}
	raw := h.readFile(t, filepath.Join(h.outDir, "evidence.json"))
	if err := json.Unmarshal([]byte(raw), &evidence); err != nil {
		t.Fatalf("the evidence is not JSON (%v):\n%s", err, raw)
	}
	if evidence.Result != "pass" {
		t.Fatalf("result = %q, want pass:\n%s", evidence.Result, raw)
	}
	if evidence.Timings.BootToConnectedMS != 1234 {
		t.Fatalf("the harness's timings did not reach the evidence:\n%s", raw)
	}
	if evidence.Artifacts.KernelSHA256 == "" || evidence.Artifacts.RootfsSHA256 == "" {
		t.Fatalf("the artefacts the run was pinned to are not recorded:\n%s", raw)
	}
	if evidence.Artifacts.RootfsBytes == 0 {
		t.Fatalf("the rootfs size is not recorded:\n%s", raw)
	}
	if evidence.RecordedAt == "" || evidence.Schema == "" {
		t.Fatalf("the evidence names neither when it was taken nor what shape it is:\n%s", raw)
	}
	// Bakeoff §12: the evidence is durations, digests and this machine's
	// shape. Nothing that could correlate a run to a place or a person.
	for _, forbidden := range []string{"hostname", "ip_address", "project"} {
		if strings.Contains(strings.ToLower(raw), forbidden) {
			t.Fatalf("the evidence carries %q, which is outside the public-data boundary:\n%s", forbidden, raw)
		}
	}
}

// TestMicrovmKVMScriptNamesEveryMissingPrecondition. An operator staging a
// feasibility host fixes what they are told about, so a check that stopped at
// the first failure would cost them one round trip per missing thing. It also
// must run NOTHING when anything is missing: a harness invoked on a half-built
// host produces a skip that looks like an answer.
func TestMicrovmKVMScriptNamesEveryMissingPrecondition(t *testing.T) {
	h := newKVMScriptHost(t)
	h.remove(t, "firecracker")
	h.remove(t, "nft")
	if err := os.Remove(filepath.Join(h.images, "vmlinux")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(h.kvm); err != nil {
		t.Fatal(err)
	}

	out, code := h.run(t)
	if code != 2 {
		t.Fatalf("script exited %d on a host missing four preconditions, want 2:\n%s", code, out)
	}
	for _, want := range []string{"firecracker", "nft", "vmlinux", h.kvm} {
		if !strings.Contains(out, want) {
			t.Fatalf("the report does not name the missing %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "4 precondition(s) missing") {
		t.Fatalf("the report does not count what is missing, so an operator cannot tell it stopped early:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(h.dir, "go-args")); err == nil {
		t.Fatal("the harness was invoked on a host that cannot run it; a skip from a half-built host looks like an answer")
	}
	if _, err := os.Stat(filepath.Join(h.outDir, "evidence.json")); err == nil {
		t.Fatal("a run that never happened wrote evidence")
	}
}

// TestMicrovmKVMScriptRefusesToCallASkipAPass. The failure this whole script
// exists to prevent: the harness skips (a missing opt-in, an unreadable
// kernel, a capability the script did not know to check), `go test` exits 0,
// and a Phase A report records ADR-0003 §8 item 1 as evidenced.
func TestMicrovmKVMScriptRefusesToCallASkipAPass(t *testing.T) {
	h := newKVMScriptHost(t)
	h.goStub = "#!/bin/sh\n" +
		"if [ \"$1\" = version ]; then echo 'go version go0.0.0-stub'; exit 0; fi\n" +
		"echo '--- SKIP: TestMicrovmBootSmokeOnKVM (0.00s)'\n" +
		"echo '    microvm_kvm_test.go:1: /dev/kvm is not usable by this process'\n" +
		"echo ok\n"

	out, code := h.run(t)
	if code != 3 {
		t.Fatalf("script exited %d for a harness that skipped, want 3:\n%s", code, out)
	}
	raw := h.readFile(t, filepath.Join(h.outDir, "evidence.json"))
	if !strings.Contains(raw, `"result": "skipped"`) {
		t.Fatalf("the evidence does not record the skip as a skip:\n%s", raw)
	}
}

// TestMicrovmKVMScriptRefusesToCallAnEmptyRunAPass. The quieter half of the
// same failure, and the one naming the tests in -run does not prevent: a test
// is renamed, the pattern no longer matches it, and `go test` exits 0 having
// run nothing and printed no SKIP. Exit status alone cannot tell that from a
// clean pass, so the script has to require positive evidence that a test
// actually ran before it writes one down.
func TestMicrovmKVMScriptRefusesToCallAnEmptyRunAPass(t *testing.T) {
	h := newKVMScriptHost(t)
	// Exits 0, prints nothing, writes no timings — `go test -run` against a
	// pattern that matches no test, exactly.
	h.goStub = "#!/bin/sh\n" +
		"if [ \"$1\" = version ]; then echo 'go version go0.0.0-stub'; exit 0; fi\n" +
		"exit 0\n"

	out, code := h.run(t)
	if code == 0 {
		t.Fatalf("script exited 0 for a run in which no test executed:\n%s", out)
	}
	raw := h.readFile(t, filepath.Join(h.outDir, "evidence.json"))
	if strings.Contains(raw, `"result": "pass"`) {
		t.Fatalf("a run in which no test executed was recorded as a pass:\n%s", raw)
	}
	if !strings.Contains(raw, `"result": "no-tests-ran"`) {
		t.Fatalf("the evidence does not say that nothing ran:\n%s", raw)
	}
	if !strings.Contains(raw, `"timings": null`) {
		t.Fatalf("a run with no timings does not say so:\n%s", raw)
	}
}

// TestMicrovmKVMScriptDoesNotCarryStaleTimingsIntoANewRun. The timings file is
// the harness's output and the evidence quotes it verbatim, so a run that
// never reached the smoke must not publish the previous run's numbers under
// this run's digests.
func TestMicrovmKVMScriptDoesNotCarryStaleTimingsIntoANewRun(t *testing.T) {
	h := newKVMScriptHost(t)
	if _, code := h.run(t); code != 0 {
		t.Fatalf("the first run exited %d", code)
	}
	if !strings.Contains(h.readFile(t, filepath.Join(h.outDir, "evidence.json")), "1234") {
		t.Fatal("the first run's timings are not in its evidence")
	}

	// A second run whose harness fails before writing any timings.
	h.goStub = "#!/bin/sh\n" +
		"if [ \"$1\" = version ]; then echo 'go version go0.0.0-stub'; exit 0; fi\n" +
		"echo '--- FAIL: TestMicrovmBootSmokeOnKVM (2.00s)'\n" +
		"exit 1\n"
	out, code := h.run(t)
	if code != 1 {
		t.Fatalf("script exited %d for a harness that failed, want 1:\n%s", code, out)
	}
	raw := h.readFile(t, filepath.Join(h.outDir, "evidence.json"))
	if strings.Contains(raw, "1234") {
		t.Fatalf("a failed run published the previous run's timings:\n%s", raw)
	}
	if !strings.Contains(raw, `"timings": null`) {
		t.Fatalf("a run with no timings does not say so:\n%s", raw)
	}
	if !strings.Contains(raw, `"result": "fail"`) {
		t.Fatalf("the evidence does not record the failure:\n%s", raw)
	}
}

// TestMicrovmKVMScriptIsTheMakeTarget. The runbook tells an operator to run
// `make microvm-kvm-test`, and a target that drifted from the script would
// make that instruction wrong in the one place nobody re-reads.
func TestMicrovmKVMScriptIsTheMakeTarget(t *testing.T) {
	makefile, err := os.ReadFile("../../Makefile")
	if err != nil {
		t.Fatal(err)
	}
	body := string(makefile)
	if !strings.Contains(body, "\nmicrovm-kvm-test:\n\t./scripts/microvm-kvm-test.sh\n") {
		t.Fatal("make microvm-kvm-test does not run scripts/microvm-kvm-test.sh")
	}
	phony := ""
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, ".PHONY:") {
			phony = line
		}
	}
	if !strings.Contains(phony, " microvm-kvm-test") {
		t.Fatalf("microvm-kvm-test is not declared .PHONY, so a file of that name would shadow it: %q", phony)
	}
	// And it is NOT part of verify: `make verify` runs on developer machines
	// and in CI, and neither has /dev/kvm.
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "verify:") && strings.Contains(line, "microvm-kvm-test") {
			t.Fatalf("verify depends on microvm-kvm-test: %q", line)
		}
	}
}

// TestMicrovmKVMHarnessNeedsAnExplicitOptIn is the other half of that promise,
// asserted about the harness rather than the script: `go test ./...` on a
// machine that happens to have /dev/kvm must still not boot a microVM.
//
// It is a statement about the source because it has to be: a test that could
// observe the harness running would be a machine that ran it.
func TestMicrovmKVMHarnessNeedsAnExplicitOptIn(t *testing.T) {
	src, err := os.ReadFile("microvm_kvm_test.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	if !strings.HasPrefix(body, "//go:build linux\n") {
		t.Fatal("the harness is not constrained to linux")
	}
	if !strings.Contains(body, `kvmOptInEnv = "RAINIER_MICROVM_KVM_TEST"`) {
		t.Fatal("the harness's opt-in variable has been renamed; scripts/microvm-kvm-test.sh sets RAINIER_MICROVM_KVM_TEST")
	}
	if !strings.Contains(body, `if v := os.Getenv(kvmOptInEnv); v != "1" {`) {
		t.Fatal("the harness no longer requires its opt-in to be exactly \"1\"")
	}
	for _, name := range []string{"TestMicrovmBootSmokeOnKVM", "TestMicrovmSatisfiesContractOnKVM"} {
		if !strings.Contains(body, "func "+name+"(t *testing.T) {\n\tfx := requireKVMHost(t)") {
			t.Fatalf("%s does not go through the precondition gate first", name)
		}
	}
	// The smoke has to come first in the file, because source order is run
	// order and a host where no guest boots should fail once rather than
	// fourteen times.
	if strings.Index(body, "func TestMicrovmBootSmokeOnKVM") > strings.Index(body, "func TestMicrovmSatisfiesContractOnKVM") {
		t.Fatal("the contract suite runs before the boot smoke; a host where no guest boots then fails in the middle of it")
	}
	if fmt.Sprint(strings.Count(body, "MicrovmOpts{")) != "1" {
		t.Fatal("the harness builds MicrovmOpts in more than one place; exactly one construction is what keeps `no Engine, Net or Format` reviewable")
	}
	for _, seam := range []string{"Engine:", "Net:", "Format:"} {
		if strings.Contains(body, "\t\t"+seam) {
			t.Fatalf("the harness injects %s — it must build the driver the production way, or it evidences nothing about Firecracker", seam)
		}
	}
}

// requireKVMHostSource returns the body of the harness's precondition gate,
// alone.
//
// Scoped to the function rather than read off the whole file because the file
// is ABOUT /dev/kvm and the jailer: its header names both, so a whole-file
// search for either would keep answering yes long after the gate stopped
// checking them.
func requireKVMHostSource(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile("microvm_kvm_test.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	const decl = "\nfunc requireKVMHost(t *testing.T) kvmFixture {\n"
	start := strings.Index(body, decl)
	if start < 0 {
		t.Fatal("the harness has no requireKVMHost gate; every KVM test goes through it first")
	}
	rest := body[start+len(decl):]
	end := strings.Index(rest, "\n}\n")
	if end < 0 {
		t.Fatal("requireKVMHost has no closing brace at column 0; this test cannot tell where the gate ends")
	}
	return rest[:end]
}

// TestMicrovmKVMHarnessGateChecksEveryPrecondition is the other half of the
// opt-in assertion: that the gate still checks the things it is a gate for.
//
// The opt-in test above pins the shape of the gate — the build tag, the exact
// "1", the order, the single MicrovmOpts — and none of that notices a gate
// that has quietly lost a check. Delete the /dev/kvm block from requireKVMHost
// and this package stays green without it: the harness only runs on a machine
// nobody runs `go test` on, so the checks it dropped are invisible here and
// the first thing that reports them missing is a Firecracker error on a
// feasibility host, naming a jail path the operator never chose.
//
// So it is asserted about the source, for the same reason the opt-in is: a
// test that could observe the gate running would be a machine that ran it.
func TestMicrovmKVMHarnessGateChecksEveryPrecondition(t *testing.T) {
	gate := requireKVMHostSource(t)
	for _, want := range []struct{ needle, why string }{
		{`"/dev/kvm"`, "a microVM session is a hardware-isolated VM and there is no software fallback"},
		{`"firecracker"`, "the VMM every session is has to be on PATH"},
		{`"jailer"`, "there is no unjailed launch path (ADR-0003 §4.5)"},
		{"effectiveCapabilities()", "the gate reads this process's capabilities"},
		{"microvmCapabilities", "and checks them against the same table the driver's own startup gate uses"},
		{`"cgroup.controllers"`, "the jailer is asked for cgroup v2 and ADR-0003 §4.6 meters each VM under it"},
	} {
		if !strings.Contains(gate, want.needle) {
			t.Fatalf("requireKVMHost no longer checks %s: %s.\nA harness that boots without it fails on a real host with a message about something else entirely.", want.needle, want.why)
		}
	}
	// And it reports them by skipping rather than failing: a developer machine
	// is not a broken KVM host.
	if !strings.Contains(gate, "t.Skipf(") {
		t.Fatal("the gate does not skip on a machine that cannot boot a microVM; `go test ./...` would then fail everywhere but a feasibility host")
	}
}
