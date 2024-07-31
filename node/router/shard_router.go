package router

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	arma "arma/core"
	"arma/node/comm"
	protos "arma/node/protos/comm"

	"google.golang.org/grpc"
)

type ShardRouter struct {
	router2batcherConnPoolSize   int
	router2batcherStreamsPerConn int
	logger                       arma.Logger
	batcherEndpoint              string
	batcherRootCAs               [][]byte
	once                         sync.Once
	lock                         sync.RWMutex
	connPool                     []*grpc.ClientConn
	streams                      [][]*stream
	tlsCert                      []byte
	tlsKey                       []byte
}

func NewShardRouter(l arma.Logger,
	batcherEndpoint string,
	batcherRootCAs [][]byte,
	tlsCert []byte,
	tlsKey []byte,
	numOfConnectionsForBatcher int,
	numOfgRPCStreamsPerConnection int,
) *ShardRouter {
	sr := &ShardRouter{
		tlsCert:                      tlsCert,
		tlsKey:                       tlsKey,
		logger:                       l,
		batcherEndpoint:              batcherEndpoint,
		batcherRootCAs:               batcherRootCAs,
		router2batcherStreamsPerConn: numOfConnectionsForBatcher,
		router2batcherConnPoolSize:   numOfgRPCStreamsPerConnection,
	}

	return sr
}

func (sr *ShardRouter) forwardBestEffort(reqID, request []byte) error {
	connIndex := int(binary.BigEndian.Uint16(reqID)) % len(sr.connPool)
	streamInConnIndex := int(binary.BigEndian.Uint16(reqID)) % sr.router2batcherStreamsPerConn

	sr.lock.RLock()
	stream := sr.streams[connIndex][streamInConnIndex]
	sr.lock.RUnlock()

	if stream == nil || stream.faulty() {
		stream = sr.maybeInitStream(connIndex, streamInConnIndex)
	}

	if stream == nil || stream.faulty() {
		return fmt.Errorf("could not establish stream to %s", sr.batcherEndpoint)
	}

	stream.requests <- &protos.Request{
		Payload: request,
	}
	return nil
}

func (sr *ShardRouter) forward(reqID, request []byte, responses chan response, trace []byte) {
	connIndex := int(binary.BigEndian.Uint16(reqID)) % len(sr.connPool)
	streamInConnIndex := int(binary.BigEndian.Uint16(reqID)) % sr.router2batcherStreamsPerConn

	sr.lock.RLock()
	stream := sr.streams[connIndex][streamInConnIndex]
	sr.lock.RUnlock()

	if stream == nil || stream.faulty() {
		stream = sr.maybeInitStream(connIndex, streamInConnIndex)
	}

	if stream == nil || stream.faulty() {
		responses <- response{
			err:   fmt.Errorf("could not establish stream to %s", sr.batcherEndpoint),
			reqID: reqID,
		}
		return
	}

	stream.registerReply(trace, responses)

	stream.requests <- &protos.Request{
		TraceId: trace,
		Payload: request,
	}
}

func (sr *ShardRouter) maybeInitStream(connIndex int, streamInConnIndex int) *stream {
	sr.lock.Lock()
	defer sr.lock.Unlock()

	stream := sr.streams[connIndex][streamInConnIndex]
	if stream == nil || stream.faulty() {
		sr.initStream(connIndex, streamInConnIndex)
	}
	stream = sr.streams[connIndex][streamInConnIndex]

	return stream
}

func (sr *ShardRouter) maybeInit() {
	sr.initConnPoolAndStreamsOnce()
	sr.maybeConnect()
}

func (sr *ShardRouter) maybeConnect() {
	if !sr.replenishNeeded() {
		return
	}

	sr.replenishConnPool()
	sr.initStreams()
}

func (sr *ShardRouter) replenishNeeded() bool {
	var createConnNeeded bool
	sr.lock.RLock()
	for _, conn := range sr.connPool {
		createConnNeeded = createConnNeeded || conn == nil
	}
	sr.lock.RUnlock()
	return createConnNeeded
}

func (sr *ShardRouter) replenishConnPool() {
	sr.lock.Lock()
	defer sr.lock.Unlock()

	cc := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			UseTLS:            true,
			ServerRootCAs:     sr.batcherRootCAs,
			Key:               sr.tlsKey,
			Certificate:       sr.tlsCert,
			RequireClientCert: true,
		},
		DialTimeout: time.Second * 5,
	}

	for i, conn := range sr.connPool {
		if conn == nil {
			conn, err := cc.Dial(sr.batcherEndpoint)
			if err != nil {
				sr.logger.Errorf("Failed connecting to %s: %v", sr.batcherEndpoint, err)
			} else {
				sr.connPool[i] = conn
			}
		}
	}
}

func (sr *ShardRouter) initStreams() {
	sr.lock.Lock()
	defer sr.lock.Unlock()

	for i := range sr.connPool {
		sr.initStreamsForConn(i)
	}
}

func (sr *ShardRouter) initStreamsForConn(i int) {
	for j := 0; j < len(sr.streams[i]); j++ {
		sr.initStream(i, j)
	}
}

func (sr *ShardRouter) initStream(i int, j int) {
	if sr.connPool[i] == nil {
		return
	}

	client := protos.NewRequestTransmitClient(sr.connPool[i])
	ctx, cancel := context.WithCancel(context.Background())
	newStream, err := client.SubmitStream(context.Background())
	if err == nil {
		s := &stream{
			endpoint:                           sr.batcherEndpoint,
			logger:                             sr.logger,
			m:                                  make(map[string]chan response),
			requests:                           make(chan *protos.Request, 1000),
			RequestTransmit_SubmitStreamClient: newStream,
			cancelThisStream:                   cancel,
			ctx:                                ctx,
		}
		go s.sendRequests()
		go s.readResponses()
		sr.streams[i][j] = s

	} else {
		sr.logger.Errorf("Failed establishing stream %d to %s: %v", i*sr.router2batcherStreamsPerConn+j, sr.batcherEndpoint, err)
		cancel()
	}
}

func (sr *ShardRouter) initConnPoolAndStreamsOnce() {
	sr.once.Do(func() {
		sr.connPool = make([]*grpc.ClientConn, sr.router2batcherConnPoolSize)
		sr.streams = make([][]*stream, sr.router2batcherConnPoolSize)
		for i := 0; i < len(sr.connPool); i++ {
			sr.streams[i] = make([]*stream, sr.router2batcherStreamsPerConn)
		}
	})
}

// func (sr *ShardRouter) getConnByIndex(connIndex int) *grpc.ClientConn {
// 	sr.lock.RLock()
// 	defer sr.lock.RUnlock()

// 	conn := sr.connPool[connIndex]
// 	if conn == nil {
// 		return nil
// 	}
// 	return conn
// }
