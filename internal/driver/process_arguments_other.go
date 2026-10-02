//go:build !linux && !darwin

package driver

import "errors"

func processArguments(int) ([]byte, error) {
	return nil, errors.New("microvm: process identity unsupported on this host")
}
func processStartTime(int) (uint64, error) {
	return 0, errors.New("microvm: process identity unsupported on this host")
}
