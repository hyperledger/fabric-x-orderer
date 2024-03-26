package node

import (
	arma "arma/pkg"
	"context"
	"crypto/rand"
	"fmt"
	"google.golang.org/grpc"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm/tlsgen"
	protos "github.ibm.com/Yacov-Manevich/ARMA/node/protos/comm"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestRouter(t *testing.T) {
	t.Skip()
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	srv, err := comm.NewGRPCServer("127.0.0.1:0", comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:      true,
			Certificate: ckp.Cert,
			Key:         ckp.Key,
		},
	})

	l := createLogger(t, 0)

	router := NewRouter(l, 8)

	var ops uint32
	router.r.Forward = func(shard uint16, request []byte) (arma.BackendError, error) {
		atomic.AddUint32(&ops, 1)
		return nil, nil
	}

	protos.RegisterRouterServer(srv.Server(), router)

	go func() {
		err := srv.Start()
		if err != nil {
			panic(err)
		}
	}()

	time.Sleep(time.Second)

	txn := make([]byte, 3)
	rand.Read(txn)

	connections := 200
	workerPerConn := 70
	workPerWorker := 100

	var wg sync.WaitGroup
	wg.Add(workerPerConn * connections)

	t1 := time.Now()
	for i := 0; i < connections; i++ {
		go func() {

			cc := comm.ClientConfig{
				SecOpts: comm.SecureOptions{
					UseTLS:        true,
					ServerRootCAs: [][]byte{ca.CertBytes()},
				},
				DialTimeout:  time.Second,
				AsyncConnect: false,
			}

			time.Sleep(time.Millisecond * 10 * time.Duration(i))
			conn, err := cc.Dial(srv.Address())
			require.NoError(t, err)

			for k := 0; k < workerPerConn; k++ {
				go invokeStream(&wg, conn, workPerWorker, txn)
			}
		}()
	}

	wg.Wait()

	opsPerformed := int(atomic.LoadUint32(&ops))
	elapsed := time.Since(t1)

	fmt.Println("total seconds:", elapsed, "total operations:", opsPerformed, "TPS:", opsPerformed/int(elapsed.Seconds()))
}

func invokeRPC(wg *sync.WaitGroup, conn *grpc.ClientConn, workPerWorker int, txn []byte) {
	defer wg.Done()
	cl := protos.NewRouterClient(conn)
	for j := 0; j < workPerWorker; j++ {
		cl.Submit(context.Background(), &protos.Request{Payload: txn})
	}
}

func invokeStream(wg *sync.WaitGroup, conn *grpc.ClientConn, workPerWorker int, txn []byte) {
	defer wg.Done()
	cl := protos.NewRouterClient(conn)
	stream, err := cl.SubmitStream(context.Background())
	if err != nil {
		panic(err)
	}

	go func() {
		for j := 0; j < workPerWorker; j++ {
			stream.Send(&protos.Request{Payload: txn})
		}
	}()

	for j := 0; j < workPerWorker; j++ {
		stream.Recv()
	}
}

func createLogger(t *testing.T, i int) *zap.SugaredLogger {
	logConfig := zap.NewDevelopmentConfig()
	logConfig.Level.SetLevel(zapcore.InfoLevel)
	logger, _ := logConfig.Build()
	logger = logger.With(zap.String("t", t.Name())).With(zap.Int64("id", int64(i)))
	sugaredLogger := logger.Sugar()
	return sugaredLogger
}
