//go:build !linux

package main

// unmountAgentHome and syncDisks on anything but Linux. Both only ever happen
// inside a microVM guest, which is Linux by construction; these exist so the
// package builds on a developer's machine.
func unmountAgentHome(string) {}

func syncDisks() {}
