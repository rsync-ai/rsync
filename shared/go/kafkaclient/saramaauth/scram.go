package saramaauth

import (
	"github.com/xdg-go/scram"
)

// scramClient adapts xdg-go/scram to sarama's SCRAMClient interface.
//
// sarama deliberately ships no SCRAM implementation: it defines the interface
// and expects the caller to supply one (see sarama's own examples). Without
// this adapter, setting Net.SASL.Mechanism to SCRAM-SHA-256/512 compiles fine
// and then panics with a nil SCRAMClientGeneratorFunc on the first handshake —
// which is why this file exists rather than being inlined.
type scramClient struct {
	*scram.Client
	*scram.ClientConversation
	hashGen scram.HashGeneratorFcn
}

func (c *scramClient) Begin(userName, password, authzID string) error {
	client, err := c.hashGen.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	c.Client = client
	c.ClientConversation = client.NewConversation()
	return nil
}

func (c *scramClient) Step(challenge string) (string, error) {
	return c.ClientConversation.Step(challenge)
}

func (c *scramClient) Done() bool {
	return c.ClientConversation.Done()
}
