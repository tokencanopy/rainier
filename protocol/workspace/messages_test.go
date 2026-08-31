package workspace_test

import (
	"encoding/json"
	"testing"

	"github.com/tokencanopy/rainier/protocol/workspace"
)

// TestChunkWireShape pins the JSON three programs exchange. Data is []byte so
// it rides as base64 with no hand-rolled encoding at any hop — the field a
// renamed tag would silently empty.
func TestChunkWireShape(t *testing.T) {
	b, err := json.Marshal(workspace.PushChunk{Xfer: "abc", Path: "repo", Seq: 3, Data: []byte("hi"), Done: true})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"xfer":"abc","path":"repo","seq":3,"data":"aGk=","done":true}`
	if string(b) != want {
		t.Fatalf("PushChunk = %s, want %s", b, want)
	}

	var back workspace.PushChunk
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(back.Data) != "hi" || back.Seq != 3 || !back.Done || back.Path != "repo" || back.Xfer != "abc" {
		t.Fatalf("round trip = %+v", back)
	}

	if b, err := json.Marshal(workspace.PushAck{Seq: 7, Synced: true}); err != nil || string(b) != `{"seq":7,"synced":true}` {
		t.Fatalf("PushAck = %s, %v", b, err)
	}
	if b, err := json.Marshal(workspace.PullRequest{Xfer: "abc", Path: "repo", Seq: 0}); err != nil ||
		string(b) != `{"xfer":"abc","path":"repo","seq":0}` {
		t.Fatalf("PullRequest = %s, %v", b, err)
	}
	if b, err := json.Marshal(workspace.PullChunk{Seq: 0, Data: []byte("hi")}); err != nil ||
		string(b) != `{"seq":0,"data":"aGk="}` {
		t.Fatalf("PullChunk = %s, %v", b, err)
	}
	if b, err := json.Marshal(workspace.DiffAnswer{}); err != nil || string(b) != `{"repos":[]}` {
		t.Fatalf("empty DiffAnswer = %s, %v; a session with no repos answers an empty array, never null", b, err)
	}
	if b, err := json.Marshal(workspace.DiffAnswer{Repos: []workspace.RepoDiff{{
		Repo: "o/n", BaseBranch: "main", SessionBranch: "rainier/x", Stat: " f | 1 +\n"}}}); err != nil ||
		string(b) != `{"repos":[{"repo":"o/n","base_branch":"main","session_branch":"rainier/x","stat":" f | 1 +\n"}]}` {
		t.Fatalf("DiffAnswer = %s, %v", b, err)
	}
}

// TestEmptyDiffAnswerIsArray pins the empty-session answer: a session with no
// repositories answers an empty array, never null, so a client branching on
// repos.length does not also have to branch on null.
func TestEmptyDiffAnswerIsArray(t *testing.T) {
	b, err := json.Marshal(workspace.DiffAnswer{})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"repos":[]}` {
		t.Fatalf("empty diff JSON = %s", b)
	}
}
