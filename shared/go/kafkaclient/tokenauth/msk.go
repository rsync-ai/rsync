package tokenauth

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// newMSKSource signs MSK IAM auth tokens from the ambient AWS credential chain.
//
// The chain is the point. On EKS, IRSA or pod identity puts a role on the pod
// and the SDK finds it with no configuration; nothing about the cluster's
// credentials is stored in this repo, in a Secret, or in an env var, and the
// role can be rotated without redeploying. That is why an MSK cluster in IAM
// auth mode — the AWS default and recommendation — could not be reached at all
// before this: it has no username and password to give.
//
// The token is a presigned kafka-cluster:Connect URL, valid for 15 minutes
// (signer.DefaultExpirySeconds). The signer reports the exact expiry, so the
// cache refreshes against the real deadline rather than a guess.
func newMSKSource(c kafkaclient.Config) Source {
	region := c.AWSRegion
	return newCached(func(ctx context.Context) (Token, error) {
		value, expiryMS, err := signer.GenerateAuthToken(ctx, region)
		if err != nil {
			return Token{}, fmt.Errorf("tokenauth: signing an MSK IAM token for region %q: %w", region, err)
		}
		return Token{Value: value, Expires: time.UnixMilli(expiryMS)}, nil
	})
}
