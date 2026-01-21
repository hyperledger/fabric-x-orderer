/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package node

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/config"
)

const (
	RouterListenPort    = 6022
	AssemblerListenPort = 6023
	BatcherListenPort   = 6024
	ConsensusListenPort = 6025
)

type ServerEndpointType uint8

const (
	undefined = iota
	AssemblerListenType
	BatcherListenType
	ConsensusListenType
	RouterListenType
)

var type2port = map[ServerEndpointType]int{
	AssemblerListenType: AssemblerListenPort,
	BatcherListenType:   BatcherListenPort,
	ConsensusListenType: ConsensusListenPort,
	RouterListenType:    RouterListenPort,
}

func ListenAddressForNode(endpointType ServerEndpointType, listenAddress string) string {
	if listenAddress == "" {
		listenAddress = "0.0.0.0"
	}
	if strings.LastIndex(listenAddress, ":") > 0 {
		return listenAddress
	}

	port, exists := type2port[endpointType]
	if !exists {
		panic(fmt.Sprintf("server listen address type %d doesn't exist", endpointType))
	}
	return net.JoinHostPort(listenAddress, fmt.Sprintf("%d", port))
}

func CreateGRPCRouter(conf *config.RouterNodeConfig) *comm.GRPCServer {
	srv, err := comm.NewGRPCServer(ListenAddressForNode(RouterListenType, conf.ListenAddress), comm.ServerConfig{
		KaOpts: comm.KeepaliveOptions{
			ServerMinInterval: time.Microsecond,
		},
		SecOpts: comm.SecureOptions{
			ClientRootCAs:     conf.ClientRootCAs,
			UseTLS:            conf.UseTLS,
			RequireClientCert: conf.ClientAuthRequired,
			Certificate:       conf.TLSCertificateFile,
			Key:               conf.TLSPrivateKeyFile,
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed running gRPC service for Router%d: %v", conf.PartyID, err)
		os.Exit(1)
	}
	return srv
}

func CreateGRPCConsensus(conf *config.ConsenterNodeConfig) *comm.GRPCServer {
	srv, err := comm.NewGRPCServer(ListenAddressForNode(ConsensusListenType, conf.ListenAddress), comm.ServerConfig{
		KaOpts: comm.KeepaliveOptions{
			ServerMinInterval: time.Microsecond,
		},
		SecOpts: comm.SecureOptions{
			ClientRootCAs:     conf.ClientRootCAs,
			ServerRootCAs:     conf.ClientRootCAs,
			RequireClientCert: true,
			UseTLS:            true,
			Certificate:       conf.TLSCertificateFile,
			Key:               conf.TLSPrivateKeyFile,
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed running gRPC service for Consensus%d: %v", conf.PartyId, err)
		os.Exit(1)
	}
	return srv
}

func CreateGRPCAssembler(conf *config.AssemblerNodeConfig) *comm.GRPCServer {
	srv, err := comm.NewGRPCServer(ListenAddressForNode(AssemblerListenType, conf.ListenAddress), comm.ServerConfig{
		KaOpts: comm.KeepaliveOptions{
			ServerMinInterval: time.Microsecond,
		},
		SecOpts: comm.SecureOptions{
			ClientRootCAs:     conf.ClientRootCAs,
			UseTLS:            conf.UseTLS,
			RequireClientCert: conf.ClientAuthRequired,
			Certificate:       conf.TLSCertificateFile,
			Key:               conf.TLSPrivateKeyFile,
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed running gRPC service for Assembler%d: %v", conf.PartyId, err)
		os.Exit(1)
	}
	return srv
}

func TLSCAcertsFromShards(shards []config.ShardInfo) [][]byte {
	marker := make(map[string]struct{})
	var tlsCAs [][]byte
	for _, shard := range shards {
		for _, batcher := range shard.Batchers {
			for _, certBundle := range batcher.TLSCACerts {
				if _, exists := marker[string(certBundle)]; !exists {
					marker[string(certBundle)] = struct{}{}
					tlsCAs = append(tlsCAs, certBundle)
				}
			}
		}
	}
	return tlsCAs
}

func TLSCAcertsFromConsenters(consenters []config.ConsenterInfo) [][]byte {
	marker := make(map[string]struct{})
	var tlsCAs [][]byte
	for _, consenter := range consenters {
		for _, tlsCA := range consenter.TLSCACerts {
			if _, exists := marker[string(tlsCA)]; !exists {
				marker[string(tlsCA)] = struct{}{}
				tlsCAs = append(tlsCAs, tlsCA)
			}
		}
	}
	return tlsCAs
}

func CreateGRPCBatcher(conf *config.BatcherNodeConfig) *comm.GRPCServer {
	srv, err := comm.NewGRPCServer(ListenAddressForNode(BatcherListenType, conf.ListenAddress), comm.ServerConfig{
		KaOpts: comm.KeepaliveOptions{
			ServerMinInterval: time.Microsecond,
		},
		SecOpts: comm.SecureOptions{
			ClientRootCAs:     conf.ClientRootCAs,
			ServerRootCAs:     conf.ClientRootCAs,
			RequireClientCert: true,
			UseTLS:            true,
			Certificate:       conf.TLSCertificateFile,
			Key:               conf.TLSPrivateKeyFile,
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed running gRPC service for Batcher%d: %v", conf.PartyId, err)
		os.Exit(1)
	}
	return srv
}
