package security

import (
	"crypto/sha256"
	"crypto/sha512"
	"hash"

	"github.com/Shopify/sarama"
	"github.com/xdg/scram"
)

var (
	// sha256HashGenerator hash generator function for SCRAM conversation
	sha256HashGenerator scram.HashGeneratorFcn = func() hash.Hash { return sha256.New() }

	// sha512HashGenerator hash generator function for SCRAM conversation
	sha512HashGenerator scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }
)

type xdgScramClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *xdgScramClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *xdgScramClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *xdgScramClient) Done() bool {
	return x.ClientConversation.Done()
}

func sha256ScramClientGeneratorFunc() sarama.SCRAMClient {
	return &xdgScramClient{HashGeneratorFcn: sha256HashGenerator}
}

func sha512ScramClientGeneratorFunc() sarama.SCRAMClient {
	return &xdgScramClient{HashGeneratorFcn: sha512HashGenerator}
}
