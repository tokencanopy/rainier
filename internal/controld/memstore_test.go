package controld_test

import (
	"testing"

	"github.com/tokencanopy/rainier/controlapp/repotest"
	"github.com/tokencanopy/rainier/internal/controld"
	"github.com/tokencanopy/rainier/internal/controld/storetest"
)

func TestMemStoreContract(t *testing.T) {
	storetest.RunContract(t, func(t *testing.T) controld.Store { return controld.NewMemStore() })
}

// TestMemStoreRepositories runs the public repository contract suite against
// the in-memory store's three native ports. They share one backing store, so
// a session created through Sessions is visible to Fleet.SessionsOnRunner —
// which is exactly what the suite checks a host for.
func TestMemStoreRepositories(t *testing.T) {
	repotest.Run(t, func(t *testing.T) repotest.Stores {
		st := controld.NewMemStore()
		return repotest.Stores{
			Sessions:     st.Sessions(),
			Environments: st.Environments(),
			Fleet:        st.Fleet(),
			Provision:    st.EnsureWorkspace,
		}
	})
}

// TestMemStoreHost runs the host-persistence suite: identity, the vault, and
// the four lookups the control ports deliberately have no method for.
func TestMemStoreHost(t *testing.T) {
	storetest.RunHost(t, func(t *testing.T) controld.HostStore { return controld.NewMemStore() })
}
