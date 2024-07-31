package router

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"arma/node/comm"
	"arma/node/comm/tlsgen"
	"arma/testutil"

	protos "arma/node/protos/comm"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type testBatcher struct {
	ops     uint32
	address string
}

func newTestBatcher(ca tlsgen.CA, t *testing.T) *testBatcher {
	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	srv, err := comm.NewGRPCServer("127.0.0.1:0", comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:      true,
			Certificate: ckp.Cert,
			Key:         ckp.Key,
		},
	})
	if err != nil {
		panic(err)
	}

	tb := &testBatcher{address: srv.Address()}

	protos.RegisterRequestTransmitServer(srv.Server(), tb)
	go func() {
		if err := srv.Start(); err != nil {
			panic(err)
		}
	}()

	return tb
}

func (t *testBatcher) Submit(ctx context.Context, request *protos.Request) (*protos.SubmitResponse, error) {
	panic("implement me")
}

func (t *testBatcher) SubmitStream(stream protos.RequestTransmit_SubmitStreamServer) error {
	responses := make(chan *protos.SubmitResponse, 1000)

	quit := make(chan struct{})

	defer close(quit)

	go func() {
		for {
			select {
			case <-quit:
				return
			case resp := <-responses:
				stream.Send(resp)
			}
		}
	}()

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		atomic.AddUint32(&t.ops, 1)

		responses <- &protos.SubmitResponse{
			TraceId: msg.TraceId,
		}
	}
}

func TestRouter(t *testing.T) {
	t.Skip()
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	testBatcher := newTestBatcher(ca, t)

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	srv, err := comm.NewGRPCServer("127.0.0.1:0", comm.ServerConfig{
		KaOpts: comm.KeepaliveOptions{
			ServerMinInterval: time.Microsecond,
		},
		SecOpts: comm.SecureOptions{
			UseTLS:      true,
			Certificate: ckp.Cert,
			Key:         ckp.Key,
		},
	})

	l := testutil.CreateLogger(t, 0)

	router := NewRouter([]uint16{1}, []string{testBatcher.address}, [][][]byte{{ca.CertBytes()}}, ckp.Cert, ckp.Key, l, 0, 0)

	protos.RegisterRequestTransmitServer(srv.Server(), router)

	go func() {
		err := srv.Start()
		if err != nil {
			panic(err)
		}
	}()

	time.Sleep(time.Second)

	connections := 50
	workerPerConn := 50
	workPerWorker := 5000

	var wg sync.WaitGroup
	wg.Add(workerPerConn * connections)

	t1 := time.Now()
	for i := 0; i < connections; i++ {
		i := i
		go func() {
			cc := comm.ClientConfig{
				SecOpts: comm.SecureOptions{
					UseTLS:        true,
					ServerRootCAs: [][]byte{ca.CertBytes()},
				},
				DialTimeout:  time.Second,
				AsyncConnect: false,
			}

			time.Sleep(time.Millisecond * 50 * time.Duration(i))
			conn, err := cc.Dial(srv.Address())
			require.NoError(t, err)

			for k := 0; k < workerPerConn; k++ {
				go invokeStream(&wg, conn, workPerWorker)
			}
		}()
	}

	wg.Wait()

	opsPerformed := int(atomic.LoadUint32(&testBatcher.ops))
	elapsed := time.Since(t1)

	fmt.Println("total seconds:", elapsed, "total operations:", opsPerformed, "TPS:", opsPerformed/int(elapsed.Seconds()))
}

// func invokeRPC(wg *sync.WaitGroup, conn *grpc.ClientConn, workPerWorker int, txn []byte) {
// 	defer wg.Done()
// 	cl := protos.NewRequestTransmitClient(conn)
// 	for j := 0; j < workPerWorker; j++ {
// 		cl.Submit(context.Background(), &protos.Request{Payload: txn})
// 	}
// }

func invokeStream(wg *sync.WaitGroup, conn *grpc.ClientConn, workPerWorker int) {
	defer wg.Done()
	cl := protos.NewRequestTransmitClient(conn)
	stream, err := cl.SubmitStream(context.Background())
	if err != nil {
		panic(err)
	}

	go func() {
		buff := make([]byte, 300)
		for j := 0; j < workPerWorker; j++ {
			binary.BigEndian.PutUint32(buff, uint32(j))
			stream.Send(&protos.Request{Payload: buff})
		}
	}()

	for j := 0; j < workPerWorker; j++ {
		resp, err := stream.Recv()
		if err != nil {
			panic(err)
		}
		if resp.Error != "" {
			panic(resp.Error)
		}
	}
}
