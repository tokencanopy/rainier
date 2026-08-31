package controld_test

import (
	"github.com/tokencanopy/rainier/internal/controld"
	"github.com/tokencanopy/rainier/internal/controld/storetest"
	"testing"
)

func TestMemStoreContract(t *testing.T) {
	storetest.RunContract(t, func(t *testing.T) controld.Store { return controld.NewMemStore() })
}
