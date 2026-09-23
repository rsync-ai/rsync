package identity

import (
	"fmt"
	"os"
	"strings"
)

// Registry holds the identity providers a binary knows about and resolves the
// configured one.
//
// It is a value rather than a package-level singleton so that a test can build
// one, and so that registration is an explicit wiring decision at construction
// rather than an init() side effect whose order nothing controls.
type Registry struct {
	providers map[string]Provider
}

// NewRegistry returns a Registry holding the given providers.
//
// It panics on a duplicate or unnamed provider. That is deliberate and is not a
// runtime failure mode: registration happens once at startup from a fixed list in
// this binary, so a duplicate is a programming error that a caller cannot handle
// and must not be allowed to resolve arbitrarily. Two providers claiming "local"
// would mean the name that selects the community default no longer identifies it.
func NewRegistry(providers ...Provider) *Registry {
	r := &Registry{providers: make(map[string]Provider, len(providers))}
	for _, p := range providers {
		name := p.Name()
		if name == "" {
			panic("identity: provider registered with an empty name")
		}
		if _, dup := r.providers[name]; dup {
			panic(fmt.Sprintf("identity: two providers registered as %q", name))
		}
		r.providers[name] = p
	}
	return r
}

// Active returns the provider that should authenticate logins right now.
//
// It reads the environment on every call rather than caching, matching
// config.BillingEnforced, so that a test can observe more than one configuration
// and so that the value an operator set is the value in force.
//
// Resolution has exactly two outcomes, and the missing third is the point:
//
//	unset or empty  -> the community default, LocalProviderName
//	a registered name -> that provider
//	anything else   -> ErrUnknownProvider, and the login is denied
//
// It never falls back to the local password provider when the configured name is
// unknown. A silent fallback turns one typo in one environment variable into an
// instance that still accepts passwords while its operator believes every login
// goes through their identity service -- authentication downgraded to a weaker
// method, with nothing in the logs saying so. Locking the instance out is loud,
// and is recovered by unsetting the variable.
func (r *Registry) Active() (Provider, error) {
	name := strings.TrimSpace(os.Getenv(ProviderEnvVar))
	if name == "" {
		name = LocalProviderName
	}
	p, ok := r.providers[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownProvider, name)
	}
	return p, nil
}

// Names returns the registered provider names, for startup logging and tests.
// The order is not defined.
func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	return names
}
