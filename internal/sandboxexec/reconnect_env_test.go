package sandboxexec

import (
	"github.com/tokencanopy/rainier/protocol/runner"
	"slices"
	"sync"
	"testing"
)

func TestReconnectReplacesFutureExecEnvironment(t *testing.T) {
	r := NewRunner(t.TempDir(), []string{"OLD_TEST_SECRET=old", "KEEP_TEST=old"}, nil)
	replace, ok := any(r).(interface{ ReplaceEnvironment([]string) })
	if !ok {
		t.Fatal("exec runner cannot replace its stale boot environment")
	}
	before, reason := r.composeEnv(runner.ExecSpec{})
	if reason != "" {
		t.Fatal(reason)
	}
	current := []string{"KEEP_TEST=current", "EMPTY_TEST="}
	replace.ReplaceEnvironment(current)
	current[0] = "KEEP_TEST=mutated"
	after, reason := r.composeEnv(runner.ExecSpec{})
	if reason != "" {
		t.Fatal(reason)
	}
	if !slices.Contains(before, "OLD_TEST_SECRET=old") || !slices.Contains(before, "KEEP_TEST=old") {
		t.Fatal("already composed environment changed")
	}
	if slices.Contains(after, "OLD_TEST_SECRET=old") || !slices.Contains(after, "KEEP_TEST=current") || !slices.Contains(after, "EMPTY_TEST=") {
		t.Fatalf("future exec environment is stale or aliased: %v", after)
	}
	after[0] = "mutated"
	again, _ := r.composeEnv(runner.ExecSpec{})
	if !slices.Contains(again, "KEEP_TEST=current") {
		t.Fatal("caller mutated stored environment")
	}
}

func TestReconnectEnvironmentReplacementIsAtomic(t *testing.T) {
	r := NewRunner(t.TempDir(), []string{"A=old", "B=old"}, nil)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			r.ReplaceEnvironment([]string{"A=new", "B=new"})
			r.ReplaceEnvironment([]string{"A=old", "B=old"})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			env, _ := r.composeEnv(runner.ExecSpec{})
			if !(slices.Equal(env, []string{"A=old", "B=old"}) || slices.Equal(env, []string{"A=new", "B=new"})) {
				t.Error("mixed environment snapshot")
				return
			}
		}
	}()
	wg.Wait()
}
