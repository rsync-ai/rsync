package identity

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// stubProvider is a provider that is not the community default, standing in for
// the out-of-process implementation this contract exists for.
type stubProvider struct {
	name string
	err  error
}

func (s stubProvider) Name() string { return s.name }

func (s stubProvider) Authenticate(context.Context, Attempt) (*Identity, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &Identity{Email: "stub@example.com"}, nil
}

func localOnly() *Registry {
	return NewRegistry(NewLocalProvider(func(context.Context, string) (string, error) {
		return "", ErrInvalidCredentials
	}))
}

func TestUnsetSelectsTheCommunityDefault(t *testing.T) {
	t.Setenv(ProviderEnvVar, "")

	p, err := localOnly().Active()
	if err != nil {
		t.Fatalf("unset %s must resolve, got %v", ProviderEnvVar, err)
	}
	if p.Name() != LocalProviderName {
		t.Fatalf("unset %s resolved to %q, want %q", ProviderEnvVar, p.Name(), LocalProviderName)
	}
}

// Whitespace is the shape a compose file or a .env line produces by accident. It
// must read as "not configured", not as a provider name of one space.
func TestWhitespaceIsNotConfigured(t *testing.T) {
	t.Setenv(ProviderEnvVar, "   ")

	p, err := localOnly().Active()
	if err != nil {
		t.Fatalf("whitespace %s must resolve to the default, got %v", ProviderEnvVar, err)
	}
	if p.Name() != LocalProviderName {
		t.Fatalf("whitespace resolved to %q, want %q", p.Name(), LocalProviderName)
	}
}

func TestConfiguredProviderIsSelectedOverTheDefault(t *testing.T) {
	t.Setenv(ProviderEnvVar, "elsewhere")

	r := NewRegistry(
		NewLocalProvider(func(context.Context, string) (string, error) { return "", ErrInvalidCredentials }),
		stubProvider{name: "elsewhere"},
	)

	p, err := r.Active()
	if err != nil {
		t.Fatalf("a registered name must resolve, got %v", err)
	}
	if p.Name() != "elsewhere" {
		t.Fatalf("selected %q, want %q", p.Name(), "elsewhere")
	}
}

// The fail-closed proof, and the reason this package exists in the shape it does.
//
// A name nothing registered is the shape of a typo, a half-finished rollout, or a
// provider that was removed from the build. Every one of those must lock the
// instance out rather than quietly authenticate everybody with a password while
// the operator believes their identity service is in the path.
func TestAnUnknownProviderDeniesAndDoesNotFallBackToLocal(t *testing.T) {
	t.Setenv(ProviderEnvVar, "sso-that-is-not-in-this-build")

	p, err := localOnly().Active()
	if err == nil {
		t.Fatalf("an unregistered provider name must not resolve; got provider %q", p.Name())
	}
	if !errors.Is(err, ErrUnknownProvider) {
		t.Fatalf("got %v, want ErrUnknownProvider", err)
	}
	if p != nil {
		t.Fatalf("a denied resolution must return no provider, got %q", p.Name())
	}
}

// The error names the value that was rejected, because an operator who mistyped
// it has to see which spelling the gateway read.
func TestUnknownProviderErrorNamesTheConfiguredValue(t *testing.T) {
	t.Setenv(ProviderEnvVar, "oidc-typo")

	_, err := localOnly().Active()
	if err == nil || !strings.Contains(err.Error(), "oidc-typo") {
		t.Fatalf("error %v does not name the configured provider", err)
	}
}

func TestRegisteringTwoProvidersUnderOneNamePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("registering a duplicate name must panic; a second provider claiming a name means that name no longer identifies one provider")
		}
	}()

	NewRegistry(stubProvider{name: "dup"}, stubProvider{name: "dup"})
}

// Specifically: nothing may shadow the community default.
func TestNothingCanRegisterOverTheCommunityDefault(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("a provider registering as %q alongside the local provider must panic", LocalProviderName)
		}
	}()

	NewRegistry(
		NewLocalProvider(func(context.Context, string) (string, error) { return "", ErrInvalidCredentials }),
		stubProvider{name: LocalProviderName},
	)
}

func TestAnUnnamedProviderPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a provider with an empty name can never be selected; registering one must panic")
		}
	}()

	NewRegistry(stubProvider{name: ""})
}

func TestNamesListsWhatIsRegistered(t *testing.T) {
	r := NewRegistry(
		NewLocalProvider(func(context.Context, string) (string, error) { return "", ErrInvalidCredentials }),
		stubProvider{name: "elsewhere"},
	)

	names := r.Names()
	if len(names) != 2 {
		t.Fatalf("Names() returned %v, want 2 entries", names)
	}
	var sawLocal bool
	for _, n := range names {
		if n == LocalProviderName {
			sawLocal = true
		}
	}
	if !sawLocal {
		t.Fatalf("Names() %v does not include the community default %q", names, LocalProviderName)
	}
}

// The declared version is what the first out-of-process implementation states
// compatibility against. An empty one is a contract that cannot be negotiated.
func TestContractVersionIsDeclared(t *testing.T) {
	if ContractVersion == "" {
		t.Fatal("ContractVersion must be declared")
	}
}
