// internal/runnerd/registry_test.go
package runnerd

import "testing"

func TestRegistryTracksAndWaits(t *testing.T) {
	r := newRegistry()
	if _, ok := r.get("s1"); ok { t.Fatal("empty registry returned a hub") }
	r.put("s1", &sessionEntry{id: "s1", state: "running"})
	e, ok := r.get("s1")
	if !ok || e.id != "s1" { t.Fatalf("get = %+v, %v", e, ok) }
	r.remove("s1")
	if _, ok := r.get("s1"); ok { t.Fatal("removed entry still present") }
}
