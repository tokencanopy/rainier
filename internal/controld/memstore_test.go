package controld_test

import (
	"rainier/internal/controld"
	"rainier/internal/controld/storetest"
	"testing"
)

func TestMemStoreContract(t *testing.T) {
	storetest.RunContract(t, func(t *testing.T) controld.Store { return controld.NewMemStore() })
}
