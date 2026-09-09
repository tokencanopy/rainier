package driver

import (
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
)

// The session image's two service helpers are shell, they run in a container
// with no docker daemon anywhere near this test, and the things that can go
// wrong with them are behavioural rather than textual: a stop that waits on the
// wrong thing and hangs, a start that binds the wrong interface, a cluster
// created with the ambient locale. So they are exercised here the way the
// smoke exercises the image — by running them — against stub binaries on PATH.
//
// They are named TestSessionImage* deliberately: the qualification workflow
// selects '^(TestSessionImage|TestImageSmoke)' against the built candidate, and
// a guard the only workflow in the repository never runs is a guard in name
// only.
//
// This is deliberately not a substitute for scripts/session-image-smoke.sh,
// which runs the real initdb and the real postmaster inside a container wearing
// the driver's restrictions. It is the half that can run on a laptop with no
// docker, which is where these scripts get edited.

func servicesHelper(t *testing.T, name string) string {
	t.Helper()
	p, err := filepath.Abs("../../images/session/services/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// stubBin writes a directory of fake executables and returns it. Each stub
// appends its own argv to $STUB_LOG before doing whatever the case needs, so a
// test can assert on the flags the helper actually passed.
func stubBin(t *testing.T, stubs map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range stubs {
		script := "#!/bin/sh\nprintf '%s %s\\n' \"$(basename \"$0\")\" \"$*\" >> \"$STUB_LOG\"\n" + body
		if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

type helperRun struct {
	out    string
	log    string
	status int
}

func runHelper(t *testing.T, helper, bin string, env []string, args ...string) helperRun {
	t.Helper()
	log := filepath.Join(t.TempDir(), "argv.log")
	if err := os.WriteFile(log, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/bin/sh", append([]string{helper}, args...)...)
	cmd.Env = append([]string{
		"PATH=" + bin + ":/usr/bin:/bin",
		"STUB_LOG=" + log,
		"HOME=" + t.TempDir(),
	}, env...)
	out, err := cmd.CombinedOutput()
	status := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			status = ee.ExitCode()
		} else {
			t.Fatalf("running %s %v: %v", helper, args, err)
		}
	}
	b, readErr := os.ReadFile(log)
	if readErr != nil {
		t.Fatal(readErr)
	}
	return helperRun{out: string(out), log: string(b), status: status}
}

func sessionUserName(t *testing.T) string {
	t.Helper()
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	return u.Username
}

// pgStubs is a PostgreSQL that exists only as far as these scripts can see it:
// initdb makes a plausible cluster, pg_ctl start writes the postmaster pidfile
// and pg_ctl stop removes it, and pg_isready answers from the pidfile.
func pgStubs(t *testing.T, data string) string {
	return stubBin(t, map[string]string{
		"initdb": `mkdir -p "` + data + `"
echo 17 > "` + data + `/PG_VERSION"
echo "# generated" > "` + data + `/postgresql.conf"
`,
		"pg_ctl": `case "$*" in
  *start*) echo running > "` + data + `/postmaster.pid" ;;
  *stop*)  rm -f "` + data + `/postmaster.pid" ;;
esac
`,
		"pg_isready": `test -f "` + data + `/postmaster.pid"`,
		"psql":       ``,
	})
}

func TestSessionImagePostgreSQLInitCreatesAUTF8ClusterAndFixesItsSocketDirectory(t *testing.T) {
	helper := servicesHelper(t, "rainier-pg")
	root := t.TempDir()
	data := filepath.Join(root, "postgresql", "data")
	sock := filepath.Join(root, "postgresql", "run")
	env := []string{"PGDATA=" + data, "PGHOST=" + sock, "PGPORT=5555"}

	got := runHelper(t, helper, pgStubs(t, data), env, "init")
	if got.status != 0 {
		t.Fatalf("init failed: %s", got.out)
	}
	// The locale is the one that matters: this image generates no locales, so
	// an initdb that inherited an unset LANG would make an SQL_ASCII database
	// that mangles the first non-ASCII row a test inserts.
	for _, want := range []string{
		"--pgdata=" + data,
		"--username=" + sessionUserName(t),
		"--encoding=UTF8",
		"--locale=C.UTF-8",
		"--auth-local=trust",
		"--auth-host=trust",
	} {
		if !strings.Contains(got.log, want) {
			t.Errorf("initdb was not passed %q; got %q", want, got.log)
		}
	}
	// PostgreSQL's compiled-in socket directory is on the read-only rootfs, so
	// a cluster that does not carry the override cannot be started by a plain
	// `pg_ctl start` — which is exactly what the documentation tells a
	// developer they can do.
	conf, err := os.ReadFile(filepath.Join(data, "postgresql.conf"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(conf), "unix_socket_directories = '"+sock+"'") {
		t.Errorf("postgresql.conf does not move the socket directory onto the workspace: %s", conf)
	}
	if !strings.Contains(string(conf), "listen_addresses = 'localhost'") {
		t.Errorf("postgresql.conf does not pin the server to loopback: %s", conf)
	}
	if _, err := os.Stat(sock); err != nil {
		t.Errorf("the socket directory was not created: %v", err)
	}
}

func TestSessionImagePostgreSQLRefusesToOverwriteAClusterOrStartWithoutOne(t *testing.T) {
	helper := servicesHelper(t, "rainier-pg")
	root := t.TempDir()
	data := filepath.Join(root, "data")
	env := []string{"PGDATA=" + data, "PGHOST=" + filepath.Join(root, "run"), "PGPORT=5555"}

	// A start with no cluster names the fix rather than producing a stack of
	// pg_ctl noise.
	got := runHelper(t, helper, pgStubs(t, data), env, "start")
	if got.status == 0 || !strings.Contains(got.out, "init") {
		t.Fatalf("start without a cluster: status=%d out=%q", got.status, got.out)
	}
	if strings.Contains(got.log, "pg_ctl") {
		t.Errorf("start ran pg_ctl anyway: %q", got.log)
	}

	if err := os.MkdirAll(data, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "PG_VERSION"), []byte("17\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got = runHelper(t, helper, pgStubs(t, data), env, "init")
	if got.status == 0 {
		t.Fatalf("init silently reinitialized an existing cluster: %q", got.out)
	}
	if strings.Contains(got.log, "initdb") {
		t.Errorf("init ran initdb over an existing cluster: %q", got.log)
	}
}

func TestSessionImagePostgreSQLStartsOnTheConfiguredPortAndStopsOnTheServersOwnSignal(t *testing.T) {
	helper := servicesHelper(t, "rainier-pg")
	root := t.TempDir()
	data := filepath.Join(root, "data")
	env := []string{"PGDATA=" + data, "PGHOST=" + filepath.Join(root, "run"), "PGPORT=5555"}
	bin := pgStubs(t, data)

	if got := runHelper(t, helper, bin, env, "init"); got.status != 0 {
		t.Fatalf("init: %s", got.out)
	}
	got := runHelper(t, helper, bin, env, "start")
	if got.status != 0 {
		t.Fatalf("start: %s", got.out)
	}
	if !strings.Contains(got.log, "--options=-p 5555") {
		t.Errorf("pg_ctl was not given the configured port: %q", got.log)
	}

	got = runHelper(t, helper, bin, env, "stop")
	if got.status != 0 {
		t.Fatalf("stop: %s", got.out)
	}
	// -W, and then a wait on the postmaster's own pidfile. pg_ctl's -w loop
	// asks kill(pid, 0), which cannot tell a shut-down postmaster from an
	// unreaped zombie, and a stop that hangs for a minute under one PID 1 and
	// not another is not worth debugging twice.
	if !strings.Contains(got.log, "-W") || !strings.Contains(got.log, "--mode=fast") {
		t.Errorf("stop did not ask for a non-blocking fast shutdown: %q", got.log)
	}
	if _, err := os.Stat(filepath.Join(data, "postmaster.pid")); !os.IsNotExist(err) {
		t.Errorf("the postmaster pidfile survived the stop: %v", err)
	}
}

// A server that never goes away has to fail loudly and quickly, not hang for
// whatever pg_ctl's default happens to be.
func TestSessionImagePostgreSQLStopFailsWhenTheServerNeverShutsDown(t *testing.T) {
	helper := servicesHelper(t, "rainier-pg")
	root := t.TempDir()
	data := filepath.Join(root, "data")
	env := []string{
		"PGDATA=" + data, "PGHOST=" + filepath.Join(root, "run"), "PGPORT=5555",
		"RAINIER_PG_TIMEOUT=2",
	}
	bin := stubBin(t, map[string]string{
		"initdb": `mkdir -p "` + data + `"; echo 17 > "` + data + `/PG_VERSION"; : > "` + data + `/postgresql.conf"`,
		// A pg_ctl whose stop does nothing at all.
		"pg_ctl":     `case "$*" in *start*) echo running > "` + data + `/postmaster.pid" ;; esac`,
		"pg_isready": `test -f "` + data + `/postmaster.pid"`,
	})
	if got := runHelper(t, helper, bin, env, "init"); got.status != 0 {
		t.Fatalf("init: %s", got.out)
	}
	if got := runHelper(t, helper, bin, env, "start"); got.status != 0 {
		t.Fatalf("start: %s", got.out)
	}
	got := runHelper(t, helper, bin, env, "stop")
	if got.status == 0 || !strings.Contains(got.out, "did not shut down") {
		t.Fatalf("a stop that never completed reported success: status=%d out=%q", got.status, got.out)
	}
}

// The DSN is the point of the whole exercise: it is what a rainier-cloud
// checkout puts in RAINIER_TEST_DATABASE_URL.
func TestSessionImagePostgreSQLPrintsTheTestSuiteDSN(t *testing.T) {
	helper := servicesHelper(t, "rainier-pg")
	env := []string{"PGDATA=/tmp/x/data", "PGHOST=/tmp/x/run", "PGPORT=5555", "PGDATABASE=postgres"}
	user := sessionUserName(t)
	for _, tc := range []struct{ args, want string }{
		{"url", "postgres://" + user + "@127.0.0.1:5555/postgres?sslmode=disable"},
		{"url cell_test", "postgres://" + user + "@127.0.0.1:5555/cell_test?sslmode=disable"},
	} {
		got := runHelper(t, helper, stubBin(t, nil), env, strings.Fields(tc.args)...)
		if strings.TrimSpace(got.out) != tc.want {
			t.Errorf("%q printed %q, want %q", tc.args, got.out, tc.want)
		}
	}
}

func TestSessionImageRedisStartsOnLoopbackInTheWorkspaceAndStopsCleanly(t *testing.T) {
	helper := servicesHelper(t, "rainier-redis")
	root := t.TempDir()
	runtime := t.TempDir()
	up := filepath.Join(root, "up")
	env := []string{
		"RAINIER_SERVICES_DIR=" + root,
		"RAINIER_SERVICES_RUNTIME_DIR=" + runtime,
		"RAINIER_REDIS_PORT=6399",
	}
	bin := stubBin(t, map[string]string{
		"redis-server": `touch "` + up + `"`,
		"redis-cli": `case "$*" in
  *shutdown*) rm -f "` + up + `" ;;
  *ping*)     if [ -f "` + up + `" ]; then echo PONG; else exit 1; fi ;;
esac
`,
	})

	got := runHelper(t, helper, bin, env, "start")
	if got.status != 0 {
		t.Fatalf("start: %s", got.out)
	}
	for _, want := range []string{
		"--daemonize yes",
		"--bind 127.0.0.1",
		"--protected-mode yes",
		"--port 6399",
		// Durable state on the volume...
		"--dir " + filepath.Join(root, "redis", "data"),
		"--logfile " + filepath.Join(root, "redis", "redis.log"),
		// ...and the socket and pidfile off it. protocol/workspace.TarGz
		// REFUSES a socket rather than skipping it, so one under /workspace
		// fails `rainier push`/`pull` of any tree containing it, and an
		// unclean exit leaves it behind to keep failing.
		"--unixsocket " + filepath.Join(runtime, "redis", "redis.sock"),
		"--pidfile " + filepath.Join(runtime, "redis", "redis.pid"),
	} {
		if !strings.Contains(got.log, want) {
			t.Errorf("redis-server was not given %q; got %q", want, got.log)
		}
	}
	if strings.Contains(got.log, "--unixsocket "+root) {
		t.Errorf("redis put its socket on the durable services volume: %q", got.log)
	}
	if _, err := os.Stat(filepath.Join(root, "redis", "data")); err != nil {
		t.Errorf("the data directory was not created on the workspace: %v", err)
	}

	if got := runHelper(t, helper, bin, env, "status"); got.status != 0 {
		t.Fatalf("status on a running server: %d %q", got.status, got.out)
	}
	got = runHelper(t, helper, bin, env, "stop")
	if got.status != 0 || !strings.Contains(got.log, "shutdown nosave") {
		t.Fatalf("stop: status=%d out=%q log=%q", got.status, got.out, got.log)
	}
	if got := runHelper(t, helper, bin, env, "status"); got.status == 0 {
		t.Errorf("status reported a stopped server as running: %q", got.out)
	}
}

func TestSessionImageRedisStopFailsWhenTheServerKeepsAnswering(t *testing.T) {
	helper := servicesHelper(t, "rainier-redis")
	root := t.TempDir()
	env := []string{
		"RAINIER_SERVICES_DIR=" + root, "RAINIER_REDIS_PORT=6399",
		"RAINIER_REDIS_TIMEOUT=2",
	}
	bin := stubBin(t, map[string]string{
		"redis-server": ``,
		"redis-cli":    `case "$*" in *ping*) echo PONG ;; esac`,
	})
	got := runHelper(t, helper, bin, env, "stop")
	if got.status == 0 || !strings.Contains(got.out, "did not shut down") {
		t.Fatalf("a stop that never completed reported success: status=%d out=%q", got.status, got.out)
	}
}

// TestSessionImagePostgreSQLStopSurvivesAStalePidfile: $PGDATA is on the workspace volume
// and outlives the container it last ran in, so the ordinary state after a
// suspend, a crash or a `docker rm` is a cluster directory with a
// postmaster.pid and no postmaster. Treating that file as proof of a running
// server makes `stop` fail — and `restart`, which is `stop` then `start`, fail
// with it — leaving the developer with a cluster they cannot bring up and no
// hint why. Worse, the recorded pid may since have been reused in the new
// container, and signalling it would interrupt something unrelated.
func TestSessionImagePostgreSQLStopSurvivesAStalePidfile(t *testing.T) {
	helper := servicesHelper(t, "rainier-pg")
	root := t.TempDir()
	data := filepath.Join(root, "data")
	env := []string{"PGDATA=" + data, "PGHOST=" + filepath.Join(root, "run"), "PGPORT=5555"}
	// Liveness is a separate marker from the pidfile, which is the whole
	// point: only a postmaster this container actually started sets it, so the
	// pre-seeded pidfile below has nothing behind it and pg_isready answers
	// with its "no response" (exit 2).
	up := filepath.Join(root, "up")
	bin := stubBin(t, map[string]string{
		"initdb": `mkdir -p "` + data + `"; echo 17 > "` + data + `/PG_VERSION"; : > "` + data + `/postgresql.conf"`,
		"pg_ctl": `case "$*" in
  *start*) touch "` + up + `"; echo running > "` + data + `/postmaster.pid" ;;
  *stop*)  rm -f "` + up + `" "` + data + `/postmaster.pid" ;;
esac`,
		"pg_isready": `test -f "` + up + `" || exit 2`,
	})
	if got := runHelper(t, helper, bin, env, "init"); got.status != 0 {
		t.Fatalf("init: %s", got.out)
	}
	if err := os.WriteFile(filepath.Join(data, "postmaster.pid"), []byte("999999\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	got := runHelper(t, helper, bin, env, "stop")
	if got.status != 0 {
		t.Fatalf("stop over a stale pidfile failed: status=%d out=%q", got.status, got.out)
	}
	if strings.Contains(got.log, "pg_ctl") {
		t.Errorf("stop signalled a pid with no server behind it: %q", got.log)
	}
	// And the pidfile is left for pg_ctl to clear, because a genuinely reused
	// pid is PostgreSQL's refusal to make and not this script's.
	if _, err := os.Stat(filepath.Join(data, "postmaster.pid")); err != nil {
		t.Errorf("stop deleted the pidfile itself: %v", err)
	}

	// restart is stop-then-start, so it has to get past the same file.
	got = runHelper(t, helper, bin, env, "restart")
	if got.status != 0 || !strings.Contains(got.log, "start") {
		t.Fatalf("restart over a stale pidfile: status=%d out=%q log=%q", got.status, got.out, got.log)
	}
}

// A server that is alive but not yet accepting connections (pg_isready's exit
// 1, "rejecting connections" — recovery, or a slow start) is a running server,
// and stop must not mistake it for a stale pidfile and walk away.
func TestSessionImagePostgreSQLStopWaitsOutAServerThatIsStillStarting(t *testing.T) {
	helper := servicesHelper(t, "rainier-pg")
	root := t.TempDir()
	data := filepath.Join(root, "data")
	env := []string{"PGDATA=" + data, "PGHOST=" + filepath.Join(root, "run"), "PGPORT=5555"}
	bin := stubBin(t, map[string]string{
		"initdb": `mkdir -p "` + data + `"; echo 17 > "` + data + `/PG_VERSION"; : > "` + data + `/postgresql.conf"`,
		"pg_ctl": `case "$*" in
  *start*) echo running > "` + data + `/postmaster.pid" ;;
  *stop*)  rm -f "` + data + `/postmaster.pid" ;;
esac`,
		"pg_isready": `test -f "` + data + `/postmaster.pid" && exit 1 || exit 2`,
	})
	if got := runHelper(t, helper, bin, env, "init"); got.status != 0 {
		t.Fatalf("init: %s", got.out)
	}
	if got := runHelper(t, helper, bin, env, "start"); got.status == 0 {
		t.Fatalf("start reported success while the server was still rejecting connections: %q", got.out)
	}
	got := runHelper(t, helper, bin, env, "stop")
	if got.status != 0 {
		t.Fatalf("stop: status=%d out=%q", got.status, got.out)
	}
	if !strings.Contains(got.log, "--mode=fast") {
		t.Errorf("stop skipped the real shutdown for a live server: %q", got.log)
	}
}

// The start deadline is the documented one, not a second number hidden in the
// script: a cluster replaying a large WAL on a slow runner needs the knob the
// help text says exists.
func TestSessionImagePostgreSQLStartHonoursItsDocumentedTimeout(t *testing.T) {
	helper := servicesHelper(t, "rainier-pg")
	root := t.TempDir()
	data := filepath.Join(root, "data")
	env := []string{
		"PGDATA=" + data, "PGHOST=" + filepath.Join(root, "run"), "PGPORT=5555",
		"RAINIER_PG_TIMEOUT=17",
	}
	bin := pgStubs(t, data)
	if got := runHelper(t, helper, bin, env, "init"); got.status != 0 {
		t.Fatalf("init: %s", got.out)
	}
	got := runHelper(t, helper, bin, env, "start")
	if got.status != 0 {
		t.Fatalf("start: %s", got.out)
	}
	if !strings.Contains(got.log, "--timeout=17") {
		t.Errorf("pg_ctl start ignored RAINIER_PG_TIMEOUT: %q", got.log)
	}
}

// TestSessionImagePostgreSQLSplitsDurableStateFromSockets: with neither PGDATA
// nor PGHOST set, the helper has to derive both from the two roots the image
// also sets — and put them on different filesystems. A cluster has to survive
// a suspend, so its data is on the workspace volume; a socket must not be on
// that volume at all, because protocol/workspace.TarGz refuses a socket rather
// than skipping it and `rainier pull` of a tree containing one fails.
func TestSessionImagePostgreSQLSplitsDurableStateFromSockets(t *testing.T) {
	helper := servicesHelper(t, "rainier-pg")
	durable, runtime := t.TempDir(), t.TempDir()
	data := filepath.Join(durable, "postgresql", "data")
	env := []string{
		"RAINIER_SERVICES_DIR=" + durable,
		"RAINIER_SERVICES_RUNTIME_DIR=" + runtime,
		"PGPORT=5555",
	}
	got := runHelper(t, helper, pgStubs(t, data), env, "init")
	if got.status != 0 {
		t.Fatalf("init: %s", got.out)
	}
	if !strings.Contains(got.log, "--pgdata="+data) {
		t.Errorf("PGDATA was not derived from RAINIER_SERVICES_DIR: %q", got.log)
	}
	sock := filepath.Join(runtime, "postgresql")
	conf, err := os.ReadFile(filepath.Join(data, "postgresql.conf"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(conf), "unix_socket_directories = '"+sock+"'") {
		t.Errorf("the socket directory was not derived from RAINIER_SERVICES_RUNTIME_DIR: %s", conf)
	}
	if strings.Contains(string(conf), "unix_socket_directories = '"+durable) {
		t.Errorf("the socket directory is on the durable volume: %s", conf)
	}
	if _, err := os.Stat(sock); err != nil {
		t.Errorf("the socket directory was not created: %v", err)
	}
}
