// Command rainier-latency measures Rainier's terminal happy path against a
// real configured fleet. It creates only synthetic scratch sessions, runs
// samples sequentially, removes every session it creates, and emits JSONL
// without session ids, user identity, runner names, or terminal contents.
package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"rainier/internal/cli"
)

const maxCapturedOutput = 32 << 20

type options struct {
	Rainier string
	Image   string
	Warmups int
	Samples int
	Cold    bool
	Timeout time.Duration
}

type observation struct {
	Type       string  `json:"type"`
	Metric     string  `json:"metric"`
	Sample     int     `json:"sample"`
	DurationMS float64 `json:"duration_ms"`
}

type summary struct {
	Type        string  `json:"type"`
	Metric      string  `json:"metric"`
	N           int     `json:"n"`
	MeanMS      float64 `json:"mean_ms"`
	StdDevMS    float64 `json:"stddev_ms"`
	P25MS       float64 `json:"p25_ms"`
	P50MS       float64 `json:"p50_ms"`
	P75MS       float64 `json:"p75_ms"`
	P95MS       float64 `json:"p95_ms"`
	MinMS       float64 `json:"min_ms"`
	MaxMS       float64 `json:"max_ms"`
	TargetP95MS float64 `json:"target_p95_ms,omitempty"`
	Pass        *bool   `json:"pass,omitempty"`
}

type meta struct {
	Type             string `json:"type"`
	Samples          int    `json:"samples"`
	Warmups          int    `json:"warmups"`
	Cold             bool   `json:"cold_suspend"`
	PercentileMethod string `json:"percentile_method"`
	Scope            string `json:"scope"`
}

func main() {
	var o options
	flag.StringVar(&o.Rainier, "rainier", "bin/rainier", "path to the rainier CLI binary")
	flag.StringVar(&o.Image, "image", "rainier-session:latest", "warmed scratch-session image")
	flag.IntVar(&o.Warmups, "warmups", 1, "unreported warm-up samples before measurement")
	flag.IntVar(&o.Samples, "samples", 10, "number of sequential samples")
	flag.BoolVar(&o.Cold, "cold", false, "include slow cold-suspend/resume samples")
	flag.DurationVar(&o.Timeout, "timeout", 90*time.Second, "timeout for one terminal operation")
	flag.Parse()

	if o.Samples < 1 || o.Samples > 100 {
		fmt.Fprintln(os.Stderr, "rainier-latency: --samples must be between 1 and 100")
		os.Exit(2)
	}
	if o.Warmups < 0 || o.Warmups > 10 {
		fmt.Fprintln(os.Stderr, "rainier-latency: --warmups must be between 0 and 10")
		os.Exit(2)
	}
	if o.Timeout <= 0 {
		fmt.Fprintln(os.Stderr, "rainier-latency: --timeout must be positive")
		os.Exit(2)
	}
	if _, err := os.Stat(o.Rainier); err != nil {
		fmt.Fprintln(os.Stderr, "rainier-latency: rainier CLI is not available at --rainier")
		os.Exit(2)
	}

	cfg, err := cli.Load()
	if err != nil || cfg.ServerURL == "" || cfg.Token == "" {
		fmt.Fprintln(os.Stderr, "rainier-latency: run `rainier login` before measuring")
		os.Exit(2)
	}
	client := &cli.Client{Base: cfg.ServerURL, Token: cfg.Token}
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(meta{
		Type:             "meta",
		Samples:          o.Samples,
		Warmups:          o.Warmups,
		Cold:             o.Cold,
		PercentileMethod: "R-7 linear interpolation",
		Scope:            "sequential warmed scratch sessions; excludes image pull, environment setup, agent boot, burst load, and geography",
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	values := map[string][]time.Duration{}
	record := func(metric string, sample int, d time.Duration) error {
		values[metric] = append(values[metric], d)
		return enc.Encode(observation{Type: "observation", Metric: metric, Sample: sample, DurationMS: milliseconds(d)})
	}
	for warmup := 1; warmup <= o.Warmups; warmup++ {
		if err := measureSample(ctx, o, client, warmup, func(string, int, time.Duration) error { return nil }); err != nil {
			fmt.Fprintf(os.Stderr, "rainier-latency: warm-up %d failed: %v\n", warmup, err)
			os.Exit(1)
		}
	}

	for sample := 1; sample <= o.Samples; sample++ {
		if err := measureSample(ctx, o, client, sample, record); err != nil {
			fmt.Fprintf(os.Stderr, "rainier-latency: sample %d failed: %v\n", sample, err)
			os.Exit(1)
		}
	}

	targets := map[string]time.Duration{
		"new_terminal_usable":         1500 * time.Millisecond,
		"attach_by_id_first_frame":    200 * time.Millisecond,
		"attach_by_id_terminal_rtt":   75 * time.Millisecond,
		"attach_by_name_first_frame":  225 * time.Millisecond,
		"attach_by_name_terminal_rtt": 75 * time.Millisecond,
	}
	metrics := make([]string, 0, len(values))
	for metric := range values {
		metrics = append(metrics, metric)
	}
	sort.Strings(metrics)
	for _, metric := range metrics {
		if err := enc.Encode(summarize(metric, values[metric], targets[metric])); err != nil {
			fmt.Fprintln(os.Stderr, "rainier-latency: writing summary failed")
			os.Exit(1)
		}
	}
}

type recordFunc func(metric string, sample int, d time.Duration) error

func measureSample(ctx context.Context, o options, client *cli.Client, sample int, record recordFunc) (retErr error) {
	suffix, err := randomHex(6)
	if err != nil {
		return fmt.Errorf("creating a synthetic session name: %w", err)
	}
	name := "latency-test-" + suffix

	created, err := startTerminal(ctx, o.Timeout, o.Rainier, "new", "--name", name, "--image", o.Image)
	if err != nil {
		return fmt.Errorf("starting rainier new: %w", err)
	}
	defer created.abort()
	started := created.started
	line, ackAt, err := created.stream.waitForLine(ctx, o.Timeout)
	if err != nil {
		return fmt.Errorf("waiting for create acknowledgement: %w", err)
	}
	id := strings.TrimSpace(line)
	if !strings.HasPrefix(id, "sess_") {
		return errors.New("create acknowledgement did not contain a session id")
	}
	defer func() {
		// Cleanup gets its own context: Ctrl-C cancels measurement but should
		// not strand the synthetic session it interrupted.
		cleanupErr := runCLI(context.Background(), o.Timeout, o.Rainier, "rm", id)
		if cleanupErr != nil && retErr == nil {
			retErr = errors.New("synthetic session cleanup failed")
		}
	}()
	if err := record("new_create_ack", sample, ackAt.Sub(started)); err != nil {
		return err
	}

	stateCh := make(chan timedResult, 1)
	go func() {
		at, err := waitRunning(ctx, client, id, o.Timeout)
		stateCh <- timedResult{at: at, err: err}
	}()
	usableAt, err := probeTerminal(ctx, created, o.Timeout)
	if err != nil {
		return fmt.Errorf("waiting for new terminal usability: %w", err)
	}
	if err := record("new_terminal_usable", sample, usableAt.Sub(started)); err != nil {
		return err
	}
	state := <-stateCh
	if state.err != nil {
		return fmt.Errorf("waiting for running state: %w", state.err)
	}
	if err := record("new_state_running", sample, state.at.Sub(started)); err != nil {
		return err
	}
	if err := created.detach(o.Timeout); err != nil {
		return fmt.Errorf("detaching new session: %w", err)
	}

	if err := measureAttach(ctx, o, sample, id, "attach_by_id", record); err != nil {
		return err
	}
	if err := measureAttach(ctx, o, sample, name, "attach_by_name", record); err != nil {
		return err
	}

	warmStart := time.Now()
	if err := runCLI(ctx, o.Timeout, o.Rainier, "suspend", id); err != nil {
		return errors.New("warm suspend failed")
	}
	if err := record("warm_suspend_command", sample, time.Since(warmStart)); err != nil {
		return err
	}
	if err := measureAttach(ctx, o, sample, id, "warm_attach", record); err != nil {
		return err
	}

	if o.Cold {
		coldStart := time.Now()
		if err := runCLI(ctx, o.Timeout, o.Rainier, "suspend", id, "--cold"); err != nil {
			return errors.New("cold suspend failed")
		}
		if err := record("cold_suspend_command", sample, time.Since(coldStart)); err != nil {
			return err
		}
		if err := measureAttach(ctx, o, sample, id, "cold_attach", record); err != nil {
			return err
		}
	}
	return nil
}

func measureAttach(ctx context.Context, o options, sample int, ref, prefix string, record recordFunc) error {
	tp, err := startTerminal(ctx, o.Timeout, o.Rainier, "attach", ref)
	if err != nil {
		return fmt.Errorf("starting %s: %w", prefix, err)
	}
	defer tp.abort()
	firstAt, err := tp.stream.waitForNew(ctx, 0, o.Timeout)
	if err != nil {
		return fmt.Errorf("waiting for %s first frame: %w", prefix, err)
	}
	if err := record(prefix+"_first_frame", sample, firstAt.Sub(tp.started)); err != nil {
		return err
	}
	probeStart := time.Now()
	usableAt, err := probeTerminal(ctx, tp, o.Timeout)
	if err != nil {
		return fmt.Errorf("waiting for %s terminal response: %w", prefix, err)
	}
	if err := record(prefix+"_terminal_rtt", sample, usableAt.Sub(probeStart)); err != nil {
		return err
	}
	return tp.detach(o.Timeout)
}

type timedResult struct {
	at  time.Time
	err error
}

type sessionEnvelope struct {
	Session struct {
		State string `json:"state"`
	} `json:"session"`
}

func waitRunning(ctx context.Context, client *cli.Client, id string, timeout time.Duration) (time.Time, error) {
	deadline := time.Now().Add(timeout)
	for {
		var resp sessionEnvelope
		if err := client.Do("GET", "/v1/sessions/"+id, nil, &resp); err != nil {
			return time.Time{}, err
		}
		if resp.Session.State == "running" {
			return time.Now(), nil
		}
		if resp.Session.State == "failed" || resp.Session.State == "dead" || resp.Session.State == "canceled" || resp.Session.State == "destroyed" {
			return time.Time{}, fmt.Errorf("session reached terminal state %s", resp.Session.State)
		}
		if !time.Now().Before(deadline) {
			return time.Time{}, errors.New("timed out")
		}
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func probeTerminal(ctx context.Context, tp *terminalProcess, timeout time.Duration) (time.Time, error) {
	suffix, err := randomHex(8)
	if err != nil {
		return time.Time{}, err
	}
	want := "rainier-ready-" + suffix
	mark := tp.stream.size()
	// The expected output never appears contiguously in the echoed command, so
	// observing it proves the remote shell executed the probe rather than merely
	// echoing local stdin through the PTY.
	command := "printf '%s%s\\n' 'rainier-ready-' '" + suffix + "'\n"
	if _, err := io.WriteString(tp.stdin, command); err != nil {
		return time.Time{}, err
	}
	return tp.stream.waitForString(ctx, mark, want, timeout)
}

func runCLI(parent context.Context, timeout time.Duration, binary string, args ...string) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("rainier %s: %w", args[0], err)
	}
	return nil
}

type outputStamp struct {
	end int
	at  time.Time
}

type terminalStream struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	stamps []outputStamp
	err    error
	notify chan struct{}
}

func newTerminalStream() *terminalStream {
	return &terminalStream{notify: make(chan struct{}, 1)}
}

func (s *terminalStream) readFrom(r io.Reader) {
	chunk := make([]byte, 32*1024)
	for {
		n, err := r.Read(chunk)
		at := time.Now()
		s.mu.Lock()
		if n > 0 && s.err == nil {
			if s.buf.Len()+n > maxCapturedOutput {
				s.err = errors.New("terminal output exceeded the benchmark capture limit")
			} else {
				s.buf.Write(chunk[:n])
				s.stamps = append(s.stamps, outputStamp{end: s.buf.Len(), at: at})
			}
		}
		if err != nil && !errors.Is(err, io.EOF) && s.err == nil {
			s.err = err
		}
		if errors.Is(err, io.EOF) && s.err == nil {
			s.err = io.EOF
		}
		s.mu.Unlock()
		select {
		case s.notify <- struct{}{}:
		default:
		}
		if err != nil {
			return
		}
	}
}

func (s *terminalStream) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Len()
}

func (s *terminalStream) waitForLine(ctx context.Context, timeout time.Duration) (string, time.Time, error) {
	var line string
	at, err := s.wait(ctx, timeout, func(data []byte, stamps []outputStamp) (time.Time, bool) {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			return time.Time{}, false
		}
		line = string(data[:i])
		return stampFor(stamps, i+1), true
	})
	return line, at, err
}

func (s *terminalStream) waitForNew(ctx context.Context, offset int, timeout time.Duration) (time.Time, error) {
	return s.wait(ctx, timeout, func(data []byte, stamps []outputStamp) (time.Time, bool) {
		if len(data) <= offset {
			return time.Time{}, false
		}
		return stampFor(stamps, offset+1), true
	})
}

func (s *terminalStream) waitForString(ctx context.Context, offset int, want string, timeout time.Duration) (time.Time, error) {
	needle := []byte(want)
	return s.wait(ctx, timeout, func(data []byte, stamps []outputStamp) (time.Time, bool) {
		if offset > len(data) {
			return time.Time{}, false
		}
		i := bytes.Index(data[offset:], needle)
		if i < 0 {
			return time.Time{}, false
		}
		return stampFor(stamps, offset+i+len(needle)), true
	})
}

func (s *terminalStream) wait(ctx context.Context, timeout time.Duration, match func([]byte, []outputStamp) (time.Time, bool)) (time.Time, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		s.mu.Lock()
		data := s.buf.Bytes()
		at, ok := match(data, s.stamps)
		err := s.err
		s.mu.Unlock()
		if ok {
			return at, nil
		}
		if err != nil {
			return time.Time{}, err
		}
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-timer.C:
			return time.Time{}, errors.New("timed out")
		case <-s.notify:
		}
	}
}

func stampFor(stamps []outputStamp, end int) time.Time {
	for _, stamp := range stamps {
		if stamp.end >= end {
			return stamp.at
		}
	}
	return time.Time{}
}

type terminalProcess struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stream  *terminalStream
	started time.Time
	wait    chan error
	once    sync.Once
}

func startTerminal(parent context.Context, timeout time.Duration, binary string, args ...string) (*terminalProcess, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	cmd := exec.CommandContext(ctx, binary, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	cmd.Stderr = io.Discard
	tp := &terminalProcess{cmd: cmd, stdin: stdin, stream: newTerminalStream(), wait: make(chan error, 1)}
	tp.started = time.Now()
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, err
	}
	go tp.stream.readFrom(stdout)
	go func() {
		err := cmd.Wait()
		cancel()
		tp.wait <- err
	}()
	return tp, nil
}

func (p *terminalProcess) detach(timeout time.Duration) error {
	if _, err := p.stdin.Write([]byte{0x1d}); err != nil {
		return err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-p.wait:
		p.once.Do(func() { _ = p.stdin.Close() })
		if err != nil {
			return fmt.Errorf("rainier attach: %w", err)
		}
		return nil
	case <-timer.C:
		p.abort()
		return errors.New("timed out waiting for detach")
	}
}

func (p *terminalProcess) abort() {
	p.once.Do(func() {
		p.stdin.Close()
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
		select {
		case <-p.wait:
		case <-time.After(5 * time.Second):
		}
	})
}

func summarize(metric string, values []time.Duration, target time.Duration) summary {
	s := summary{Type: "summary", Metric: metric, N: len(values), TargetP95MS: milliseconds(target)}
	if len(values) == 0 {
		return s
	}
	sorted := append([]time.Duration(nil), values...)
	slices.Sort(sorted)
	var sum float64
	for _, value := range sorted {
		sum += float64(value)
	}
	mean := sum / float64(len(sorted))
	var squares float64
	for _, value := range sorted {
		delta := float64(value) - mean
		squares += delta * delta
	}
	s.MeanMS = milliseconds(time.Duration(mean))
	s.StdDevMS = milliseconds(time.Duration(math.Sqrt(squares / float64(len(sorted)))))
	s.P25MS = milliseconds(percentile(sorted, 0.25))
	s.P50MS = milliseconds(percentile(sorted, 0.50))
	s.P75MS = milliseconds(percentile(sorted, 0.75))
	p95 := percentile(sorted, 0.95)
	s.P95MS = milliseconds(p95)
	s.MinMS = milliseconds(sorted[0])
	s.MaxMS = milliseconds(sorted[len(sorted)-1])
	if target > 0 {
		pass := p95 <= target
		s.Pass = &pass
	}
	return s
}

// percentile uses the R-7 definition used by Go-adjacent analysis tools and
// common spreadsheets: rank=(n-1)*p, linearly interpolated between neighbors.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := float64(len(sorted)-1) * p
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return sorted[lo]
	}
	fraction := rank - float64(lo)
	return sorted[lo] + time.Duration(float64(sorted[hi]-sorted[lo])*fraction)
}

func milliseconds(d time.Duration) float64 {
	return math.Round(float64(d)/float64(time.Millisecond)*1000) / 1000
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := crand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
