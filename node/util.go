package node

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"node/comm"
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

var (
	type2port = map[ServerEndpointType]int{
		AssemblerListenType: AssemblerListenPort,
		BatcherListenType:   BatcherListenPort,
		ConsensusListenType: ConsensusListenPort,
		RouterListenType:    RouterListenPort,
	}
)

func ListenAddressForNode(endpointType ServerEndpointType, listenAddress string) string {
	if listenAddress == "" {
		listenAddress = "0.0.0.0"
	}
	if strings.LastIndex(listenAddress, ":") > 0 {
		return listenAddress
	}

	port, exists := type2port[endpointType]
	if !exists {
		panic(fmt.Sprintf("server listen adress type %d doesn't exist", endpointType))
	}
	return net.JoinHostPort(listenAddress, fmt.Sprintf("%d", port))
}

func CreateGRPCRouter(conf RouterNodeConfig) *comm.GRPCServer {
	tlsCAs := TLSCAcertsFromShards(conf.Shards)

	srv, err := comm.NewGRPCServer(ListenAddressForNode(RouterListenType, conf.ListenAddress), comm.ServerConfig{
		KaOpts: comm.KeepaliveOptions{
			ServerMinInterval: time.Microsecond,
		},
		SecOpts: comm.SecureOptions{
			ClientRootCAs:     tlsCAs,
			UseTLS:            true,
			RequireClientCert: true,
			Certificate:       conf.TLSCertificateFile,
			Key:               conf.TLSPrivateKeyFile,
		},
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed running gRPC service: %v", err)
		os.Exit(1)
	}
	return srv
}

func CreateGRPCConsensus(conf ConsenterNodeConfig) *comm.GRPCServer {
	var clientRootCAs [][]byte

	for _, shard := range conf.Shards {
		for _, batchers := range shard.Batchers {
			for _, tlsCA := range batchers.TLSCACerts {
				clientRootCAs = append(clientRootCAs, tlsCA)
			}
		}
	}

	for _, consenter := range conf.Consenters {
		for _, tlsCA := range consenter.TLSCACerts {
			clientRootCAs = append(clientRootCAs, tlsCA)
		}
	}

	srv, err := comm.NewGRPCServer(ListenAddressForNode(ConsensusListenType, conf.ListenAddress), comm.ServerConfig{
		KaOpts: comm.KeepaliveOptions{
			ServerMinInterval: time.Microsecond,
		},
		SecOpts: comm.SecureOptions{
			ClientRootCAs:     clientRootCAs,
			ServerRootCAs:     clientRootCAs,
			RequireClientCert: true,
			UseTLS:            true,
			Certificate:       conf.TLSCertificateFile,
			Key:               conf.TLSPrivateKeyFile,
		},
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed running gRPC service: %v", err)
		os.Exit(1)
	}
	return srv
}

func CreateGRPCAssembler(conf AssemblerNodeConfig) *comm.GRPCServer {
	tlsCAs := TLSCAcertsFromShards(conf.Shards)

	srv, err := comm.NewGRPCServer(ListenAddressForNode(AssemblerListenType, conf.ListenAddress), comm.ServerConfig{
		KaOpts: comm.KeepaliveOptions{
			ServerMinInterval: time.Microsecond,
		},
		SecOpts: comm.SecureOptions{
			ClientRootCAs:     tlsCAs,
			UseTLS:            true,
			RequireClientCert: true,
			Certificate:       conf.TLSCertificateFile,
			Key:               conf.TLSPrivateKeyFile,
		},
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed running gRPC service: %v", err)
		os.Exit(1)
	}
	return srv
}

func TLSCAcertsFromShards(shards []ShardInfo) [][]byte {
	var tlsCAs [][]byte
	for _, shard := range shards {
		for _, batcher := range shard.Batchers {
			for _, certBundle := range batcher.TLSCACerts {
				tlsCAs = append(tlsCAs, certBundle)
			}
		}
	}
	return tlsCAs
}

func CreateGRPCBatcher(conf BatcherNodeConfig) *comm.GRPCServer {
	var clientRootCAs [][]byte

	for _, shard := range conf.Shards {
		if shard.ShardId != conf.ShardId {
			continue
		}
		for _, batchers := range shard.Batchers {
			for _, tlsCA := range batchers.TLSCACerts {
				clientRootCAs = append(clientRootCAs, tlsCA)
			}
		}
	}

	srv, err := comm.NewGRPCServer(ListenAddressForNode(BatcherListenType, conf.ListenAddress), comm.ServerConfig{
		KaOpts: comm.KeepaliveOptions{
			ServerMinInterval: time.Microsecond,
		},
		SecOpts: comm.SecureOptions{
			ClientRootCAs:     clientRootCAs,
			ServerRootCAs:     clientRootCAs,
			RequireClientCert: true,
			UseTLS:            true,
			Certificate:       conf.TLSCertificateFile,
			Key:               conf.TLSPrivateKeyFile,
		},
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed running gRPC service: %v", err)
		os.Exit(1)
	}
	return srv
}
