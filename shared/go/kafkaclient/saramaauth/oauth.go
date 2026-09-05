package saramaauth

import (
	"context"

	"github.com/IBM/sarama"

	"github.com/rsync-ai/shared/kafkaclient/tokenauth"
)

// tokenProvider adapts a tokenauth.Source to sarama's AccessTokenProvider.
//
// sarama calls Token() once per broker connection and documents two
// expectations that the source already satisfies: reuse a token across the
// connections opened at startup, and never return an expired one. The adapter
// is thin on purpose — putting the refresh logic here would mean writing it
// again for kafka-go.
type tokenProvider struct {
	src tokenauth.Source
}

// Token satisfies sarama.AccessTokenProvider.
//
// The interface has no context, so the source supplies its own deadline
// (tokenauth.SignTimeout) rather than blocking a broker connection forever on a
// quiet STS or IdP endpoint.
func (p tokenProvider) Token() (*sarama.AccessToken, error) {
	t, err := p.src.Token(context.Background())
	if err != nil {
		return nil, err
	}
	return &sarama.AccessToken{Token: t.Value, Extensions: t.Extensions}, nil
}
