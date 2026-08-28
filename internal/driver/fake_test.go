// internal/driver/fake_test.go
package driver

import "testing"

func TestFakeSatisfiesContract(t *testing.T) {
	RunContract(t, func(t *testing.T) (Driver, func()) {
		d := NewFake(4)
		return d, func() {}
	})
}
