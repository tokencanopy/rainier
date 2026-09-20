//go:build !linux

package main

// unmountAgentHome on anything but Linux. A cold suspend only ever happens
// to a microVM guest, which is Linux by construction; this exists so the
// package builds on a developer's machine.
func unmountAgentHome(string) {}
