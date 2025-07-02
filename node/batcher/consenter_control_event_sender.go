/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"

	"github.com/hyperledger/fabric-x-orderer/core"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"

	"github.com/pkg/errors"
)

//go:generate counterfeiter -o mocks/consenter_control_event_sender_creator.go . ConsenterControlEventSenderCreator
type ConsenterControlEventSenderCreator interface {
	CreateConsenterControlEventSender(TLSPrivateKeyFile config.RawBytes, TLSCertificateFile config.RawBytes, consenterInfo config.ConsenterInfo) ConsenterControlEventSender
}

type ConsenterControlEventSenderFactory struct{}

func (f *ConsenterControlEventSenderFactory) CreateConsenterControlEventSender(TLSPrivateKey config.RawBytes, TLSCertificate config.RawBytes, consenterInfo config.ConsenterInfo) ConsenterControlEventSender {
	return &ConsenterControlEventStream{ConsenterInfo: consenterInfo, TLSPrivateKey: TLSPrivateKey, TLSCertificate: TLSCertificate}
}

//go:generate counterfeiter -o mocks/consenter_control_event_sender.go . ConsenterControlEventSender
type ConsenterControlEventSender interface {
	SendControlEvent(ce core.ControlEvent) error
}

type ConsenterControlEventStream struct {
	stream         protos.Consensus_NotifyEventClient
	ConsenterInfo  config.ConsenterInfo
	TLSPrivateKey  config.RawBytes
	TLSCertificate config.RawBytes
}

func CreateConsenterControlEventStream(TLSPrivateKey config.RawBytes, TLSCertificate config.RawBytes, consenterInfo config.ConsenterInfo) *ConsenterControlEventStream {
	s := &ConsenterControlEventStream{ConsenterInfo: consenterInfo, TLSPrivateKey: TLSPrivateKey, TLSCertificate: TLSCertificate}
	s.createStream() // TODO do something when this returns an error
	return s
}

func (s *ConsenterControlEventStream) createStream() error {
	cc := clientConfig(s.TLSPrivateKey, s.TLSCertificate, s.ConsenterInfo.TLSCACerts)
	conn, err := cc.Dial(s.ConsenterInfo.Endpoint)
	if err != nil {
		return errors.Errorf("Failed connecting to %s: %v", s.ConsenterInfo.Endpoint, err)
	}
	cl := protos.NewConsensusClient(conn)
	s.stream, err = cl.NotifyEvent(context.Background())
	if err != nil {
		return errors.Errorf("Failed creating notify even client: %v", err)
	}
	return nil
}

func (s *ConsenterControlEventStream) SendControlEvent(ce core.ControlEvent) error {
	if s.stream == nil {
		if err := s.createStream(); err != nil {
			s.stream = nil // TODO maybe retry
			return errors.Errorf("Failed creating stream to consenter %d (endpoint: %s): %v", s.ConsenterInfo.PartyID, s.ConsenterInfo.Endpoint, err)
		}
	}
	err := s.stream.Send(&protos.Event{
		Payload: ce.Bytes(),
	})
	if err != nil { // TODO maybe retry
		s.stream = nil
		return errors.Errorf("Failed sending control event to consenter %d (endpoint: %s); error: %v", s.ConsenterInfo.PartyID, s.ConsenterInfo.Endpoint, err)
	}
	return nil
}
