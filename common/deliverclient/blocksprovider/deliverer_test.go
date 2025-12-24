/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blocksprovider_test

import (
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/bccsp/sw"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/hyperledger/fabric-x-common/common/crypto/tlsgen"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/tools/configtxgen"
	"github.com/hyperledger/fabric-x-common/tools/test"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/blocksprovider"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/blocksprovider/fake"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/orderers"
	arma_testutil "github.com/hyperledger/fabric-x-orderer/testutil/fabric"
)

const eventuallyTO = 20 * time.Second

var _ = ginkgo.Describe("CFT-Deliverer", func() {
	var (
		d                                  *blocksprovider.Deliverer
		ccs                                []*grpc.ClientConn
		fakeDialer                         *fake.Dialer
		fakeBlockHandler                   *fake.BlockHandler
		fakeOrdererConnectionSource        *fake.OrdererConnectionSource
		fakeOrdererConnectionSourceFactory *fake.OrdererConnectionSourceFactory
		fakeLedgerInfo                     *fake.LedgerInfo
		fakeUpdatableBlockVerifier         *fake.UpdatableBlockVerifier
		fakeSigner                         *fake.Signer
		fakeDeliverStreamer                *fake.DeliverStreamer
		fakeDeliverClient                  *fake.DeliverClient
		fakeSleeper                        *fake.Sleeper
		fakeDurationExceededHandler        *fake.DurationExceededHandler
		fakeCryptoProvider                 bccsp.BCCSP
		doneC                              chan struct{}
		recvStep                           chan struct{}
		endC                               chan struct{}
		mutex                              sync.Mutex
		tempDir                            string
		channelConfig                      *common.Config
	)

	ginkgo.BeforeEach(func() {
		var err error
		tempDir, err = os.MkdirTemp("", "deliverer")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		doneC = make(chan struct{})
		recvStep = make(chan struct{})

		// appease the race detector
		recvStep := recvStep
		doneC := doneC

		fakeDialer = &fake.Dialer{}
		ccs = nil
		fakeDialer.DialStub = func(string, [][]byte) (*grpc.ClientConn, error) {
			mutex.Lock()
			defer mutex.Unlock()
			cc, err := grpc.Dial("localhost:6006", grpc.WithTransportCredentials(insecure.NewCredentials()))
			ccs = append(ccs, cc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(cc.GetState()).NotTo(gomega.Equal(connectivity.Shutdown))
			return cc, nil
		}

		fakeBlockHandler = &fake.BlockHandler{}
		fakeUpdatableBlockVerifier = &fake.UpdatableBlockVerifier{}
		fakeSigner = &fake.Signer{}

		fakeLedgerInfo = &fake.LedgerInfo{}
		fakeLedgerInfo.LedgerHeightReturns(7, nil)

		fakeOrdererConnectionSource = &fake.OrdererConnectionSource{}
		fakeOrdererConnectionSource.RandomEndpointReturns(&orderers.Endpoint{
			Address: "orderer-address",
		}, nil)

		fakeOrdererConnectionSourceFactory = &fake.OrdererConnectionSourceFactory{}
		fakeOrdererConnectionSourceFactory.CreateConnectionSourceReturns(fakeOrdererConnectionSource)

		fakeDeliverClient = &fake.DeliverClient{}
		fakeDeliverClient.RecvStub = func() (*orderer.DeliverResponse, error) {
			select {
			case <-recvStep:
				return nil, fmt.Errorf("fake-recv-step-error")
			case <-doneC:
				return nil, nil
			}
		}

		fakeDeliverClient.CloseSendStub = func() error {
			select {
			case recvStep <- struct{}{}:
			case <-doneC:
			}
			return nil
		}

		fakeDeliverStreamer = &fake.DeliverStreamer{}
		fakeDeliverStreamer.DeliverReturns(fakeDeliverClient, nil)

		fakeDurationExceededHandler = &fake.DurationExceededHandler{}
		fakeDurationExceededHandler.DurationExceededHandlerReturns(false)

		channelConfig, fakeCryptoProvider, err = testSetup(tempDir, "CFT")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		d = &blocksprovider.Deliverer{
			ChannelID:                       "channel-id",
			BlockHandler:                    fakeBlockHandler,
			Ledger:                          fakeLedgerInfo,
			UpdatableBlockVerifier:          fakeUpdatableBlockVerifier,
			Dialer:                          fakeDialer,
			OrderersSourceFactory:           fakeOrdererConnectionSourceFactory,
			CryptoProvider:                  fakeCryptoProvider,
			DoneC:                           make(chan struct{}),
			Signer:                          fakeSigner,
			DeliverStreamer:                 fakeDeliverStreamer,
			Logger:                          flogging.MustGetLogger("blocksprovider"),
			TLSCertHash:                     []byte("tls-cert-hash"),
			MaxRetryDuration:                time.Hour,
			MaxRetryDurationExceededHandler: fakeDurationExceededHandler.DurationExceededHandler,
			MaxRetryInterval:                10 * time.Second,
			InitialRetryInterval:            100 * time.Millisecond,
		}
		d.Initialize(channelConfig)
		fakeSleeper = &fake.Sleeper{}
		blocksprovider.SetSleeper(d, fakeSleeper)
	})

	ginkgo.JustBeforeEach(func() {
		endC = make(chan struct{})
		go func() {
			d.DeliverBlocks()
			close(endC)
		}()
	})

	ginkgo.AfterEach(func() {
		d.Stop()
		close(doneC)
		<-endC

		_ = os.RemoveAll(tempDir)
	})

	ginkgo.It("waits patiently for new blocks from the orderer", func() {
		gomega.Consistently(endC).ShouldNot(gomega.BeClosed())
		mutex.Lock()
		defer mutex.Unlock()
		gomega.Expect(ccs[0].GetState()).NotTo(gomega.Equal(connectivity.Shutdown))
	})

	ginkgo.It("checks the ledger height", func() {
		gomega.Eventually(fakeLedgerInfo.LedgerHeightCallCount, eventuallyTO).Should(gomega.Equal(1))
	})

	ginkgo.When("the ledger returns an error", func() {
		ginkgo.BeforeEach(func() {
			fakeLedgerInfo.LedgerHeightReturns(0, fmt.Errorf("fake-ledger-error"))
		})

		ginkgo.It("exits the loop", func() {
			gomega.Eventually(endC, eventuallyTO).Should(gomega.BeClosed())
		})
	})

	ginkgo.It("signs the seek info request", func() {
		gomega.Eventually(fakeSigner.SignCallCount, eventuallyTO).Should(gomega.Equal(1))
		// Note, the signer is used inside a util method
		// which has its own set of tests, so checking the args
		// in this test is unnecessary
	})

	ginkgo.When("the signer returns an error", func() {
		ginkgo.BeforeEach(func() {
			fakeSigner.SignReturns(nil, fmt.Errorf("fake-signer-error"))
		})

		ginkgo.It("exits the loop", func() {
			gomega.Eventually(endC, eventuallyTO).Should(gomega.BeClosed())
		})
	})

	ginkgo.It("gets a random endpoint to connect to from the orderer connection source", func() {
		gomega.Eventually(fakeOrdererConnectionSource.RandomEndpointCallCount,
			eventuallyTO).Should(gomega.Equal(1))
	})

	ginkgo.When("the orderer connection source returns an error", func() {
		ginkgo.BeforeEach(func() {
			fakeOrdererConnectionSource.RandomEndpointReturnsOnCall(0, nil, fmt.Errorf("fake-endpoint-error"))
			fakeOrdererConnectionSource.RandomEndpointReturnsOnCall(1, &orderers.Endpoint{
				Address: "orderer-address",
			}, nil)
		})

		ginkgo.It("sleeps and retries until a valid endpoint is selected", func() {
			gomega.Eventually(fakeOrdererConnectionSource.RandomEndpointCallCount, eventuallyTO).Should(gomega.Equal(2))
			gomega.Expect(fakeSleeper.SleepCallCount()).To(gomega.Equal(1))
			gomega.Expect(fakeSleeper.SleepArgsForCall(0)).To(gomega.Equal(100 * time.Millisecond))
		})
	})

	ginkgo.When("the orderer connect is refreshed", func() {
		ginkgo.BeforeEach(func() {
			refreshedC := make(chan struct{})
			close(refreshedC)
			fakeOrdererConnectionSource.RandomEndpointReturnsOnCall(0, &orderers.Endpoint{
				Address:   "orderer-address",
				Refreshed: refreshedC,
			}, nil)
			fakeOrdererConnectionSource.RandomEndpointReturnsOnCall(1, &orderers.Endpoint{
				Address: "orderer-address",
			}, nil)
		})

		ginkgo.It("does not sleep, but disconnects and immediately tries to reconnect", func() {
			gomega.Eventually(fakeOrdererConnectionSource.RandomEndpointCallCount, eventuallyTO).Should(gomega.Equal(2))
			gomega.Expect(fakeSleeper.SleepCallCount()).To(gomega.Equal(0))
		})
	})

	ginkgo.It("dials the random endpoint", func() {
		gomega.Eventually(fakeDialer.DialCallCount, eventuallyTO).Should(gomega.Equal(1))
		addr, tlsCerts := fakeDialer.DialArgsForCall(0)
		gomega.Expect(addr).To(gomega.Equal("orderer-address"))
		gomega.Expect(tlsCerts).To(gomega.BeNil()) // TODO
	})

	ginkgo.When("the dialer returns an error", func() {
		ginkgo.BeforeEach(func() {
			fakeDialer.DialReturnsOnCall(0, nil, fmt.Errorf("fake-dial-error"))
			cc, err := grpc.Dial("localhost:6006", grpc.WithTransportCredentials(insecure.NewCredentials()))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			fakeDialer.DialReturnsOnCall(1, cc, nil)
		})

		ginkgo.It("sleeps and retries until dial is successful", func() {
			gomega.Eventually(fakeDialer.DialCallCount, eventuallyTO).Should(gomega.Equal(2))
			gomega.Expect(fakeSleeper.SleepCallCount()).To(gomega.Equal(1))
			gomega.Expect(fakeSleeper.SleepArgsForCall(0)).To(gomega.Equal(100 * time.Millisecond))
		})
	})

	ginkgo.It("constructs a deliver client", func() {
		gomega.Eventually(fakeDeliverStreamer.DeliverCallCount, eventuallyTO).Should(gomega.Equal(1))
	})

	ginkgo.When("the deliver client cannot be created", func() {
		ginkgo.BeforeEach(func() {
			fakeDeliverStreamer.DeliverReturnsOnCall(0, nil, fmt.Errorf("deliver-error"))
			fakeDeliverStreamer.DeliverReturnsOnCall(1, fakeDeliverClient, nil)
		})

		ginkgo.It("closes the grpc connection, sleeps, and tries again", func() {
			gomega.Eventually(fakeDeliverStreamer.DeliverCallCount, eventuallyTO).Should(gomega.Equal(2))
			gomega.Expect(fakeSleeper.SleepCallCount()).To(gomega.Equal(1))
			gomega.Expect(fakeSleeper.SleepArgsForCall(0)).To(gomega.Equal(100 * time.Millisecond))
		})
	})

	//nolint:dupl // 425-440 lines are duplicate of 285-300.
	ginkgo.When("there are consecutive errors", func() {
		ginkgo.BeforeEach(func() {
			fakeDeliverStreamer.DeliverReturnsOnCall(0, nil, fmt.Errorf("deliver-error"))
			fakeDeliverStreamer.DeliverReturnsOnCall(1, nil, fmt.Errorf("deliver-error"))
			fakeDeliverStreamer.DeliverReturnsOnCall(2, nil, fmt.Errorf("deliver-error"))
			fakeDeliverStreamer.DeliverReturnsOnCall(3, fakeDeliverClient, nil)
		})

		ginkgo.It("sleeps in an exponential fashion and retries until dial is successful", func() {
			gomega.Eventually(fakeDeliverStreamer.DeliverCallCount, eventuallyTO).Should(gomega.Equal(4))
			gomega.Expect(fakeSleeper.SleepCallCount()).To(gomega.Equal(3))
			gomega.Expect(fakeSleeper.SleepArgsForCall(0)).To(gomega.Equal(100 * time.Millisecond))
			gomega.Expect(fakeSleeper.SleepArgsForCall(1)).To(gomega.Equal(120 * time.Millisecond))
			gomega.Expect(fakeSleeper.SleepArgsForCall(2)).To(gomega.Equal(144 * time.Millisecond))
		})
	})

	ginkgo.When("the consecutive errors are unbounded and the peer is not a static leader", func() {
		ginkgo.BeforeEach(func() {
			fakeDurationExceededHandler.DurationExceededHandlerReturns(true)
			fakeDeliverStreamer.DeliverReturns(nil, fmt.Errorf("deliver-error"))
			fakeDeliverStreamer.DeliverReturnsOnCall(500, fakeDeliverClient, nil)
		})

		ginkgo.It("hits the maximum sleep time value in an exponential fashion and retries until exceeding "+
			"the max retry duration", func() {
			gomega.Eventually(fakeDurationExceededHandler.DurationExceededHandlerCallCount, eventuallyTO).Should(
				gomega.BeNumerically(">", 0),
			)
			gomega.Eventually(endC, eventuallyTO).Should(gomega.BeClosed())
			gomega.Eventually(fakeSleeper.SleepCallCount, eventuallyTO).Should(gomega.Equal(380))
			gomega.Expect(fakeSleeper.SleepArgsForCall(25)).To(gomega.Equal(9539 * time.Millisecond))
			gomega.Expect(fakeSleeper.SleepArgsForCall(26)).To(gomega.Equal(10 * time.Second))
			gomega.Expect(fakeSleeper.SleepArgsForCall(27)).To(gomega.Equal(10 * time.Second))
			gomega.Expect(fakeSleeper.SleepArgsForCall(379)).To(gomega.Equal(10 * time.Second))
			gomega.Expect(fakeDurationExceededHandler.DurationExceededHandlerCallCount()).Should(gomega.Equal(1))
		})
	})

	ginkgo.When("the consecutive errors are coming in short bursts and the peer is not a static leader", func() {
		ginkgo.BeforeEach(func() {
			// appease the race detector
			doneC := doneC
			recvStep := recvStep
			fakeDeliverClient := fakeDeliverClient

			fakeDeliverClient.CloseSendStub = func() error {
				if fakeDeliverClient.CloseSendCallCount() >= 1000 {
					select {
					case <-doneC:
					case recvStep <- struct{}{}:
					}
				}
				return nil
			}
			fakeDeliverClient.RecvStub = func() (*orderer.DeliverResponse, error) {
				c := fakeDeliverClient.RecvCallCount()
				switch c {
				case 300:
					return &orderer.DeliverResponse{
						Type: &orderer.DeliverResponse_Block{
							Block: &common.Block{
								Header: &common.BlockHeader{
									Number: 8,
								},
							},
						},
					}, nil
				case 600:
					return &orderer.DeliverResponse{
						Type: &orderer.DeliverResponse_Block{
							Block: &common.Block{
								Header: &common.BlockHeader{
									Number: 9,
								},
							},
						},
					}, nil
				case 900:
					return &orderer.DeliverResponse{
						Type: &orderer.DeliverResponse_Block{
							Block: &common.Block{
								Header: &common.BlockHeader{
									Number: 9,
								},
							},
						},
					}, nil
				default:
					if c < 900 {
						return nil, fmt.Errorf("fake-recv-error-XXX")
					}

					select {
					case <-recvStep:
						return nil, fmt.Errorf("fake-recv-step-error-XXX")
					case <-doneC:
						return nil, nil
					}
				}
			}
			fakeDurationExceededHandler.DurationExceededHandlerReturns(true)
		})

		ginkgo.It("hits the maximum sleep time value in an exponential fashion and retries but does not "+
			"exceed the max retry duration", func() {
			gomega.Eventually(fakeSleeper.SleepCallCount, eventuallyTO).Should(gomega.Equal(897))
			gomega.Expect(fakeSleeper.SleepArgsForCall(0)).To(gomega.Equal(100 * time.Millisecond))
			gomega.Expect(fakeSleeper.SleepArgsForCall(25)).To(gomega.Equal(9539 * time.Millisecond))
			gomega.Expect(fakeSleeper.SleepArgsForCall(26)).To(gomega.Equal(10 * time.Second))
			gomega.Expect(fakeSleeper.SleepArgsForCall(27)).To(gomega.Equal(10 * time.Second))
			gomega.Expect(fakeSleeper.SleepArgsForCall(298)).To(gomega.Equal(10 * time.Second))
			gomega.Expect(fakeSleeper.SleepArgsForCall(299)).To(gomega.Equal(100 * time.Millisecond))
			gomega.Expect(fakeSleeper.SleepArgsForCall(2*299 - 1)).To(gomega.Equal(10 * time.Second))
			gomega.Expect(fakeSleeper.SleepArgsForCall(2 * 299)).To(gomega.Equal(100 * time.Millisecond))
			gomega.Expect(fakeSleeper.SleepArgsForCall(3*299 - 1)).To(gomega.Equal(10 * time.Second))

			gomega.Expect(fakeDurationExceededHandler.DurationExceededHandlerCallCount()).Should(gomega.Equal(0))
		})
	})

	ginkgo.When("the consecutive errors are unbounded and the peer is static leader", func() {
		ginkgo.BeforeEach(func() {
			fakeDeliverStreamer.DeliverReturns(nil, fmt.Errorf("deliver-error"))
			fakeDeliverStreamer.DeliverReturnsOnCall(500, fakeDeliverClient, nil)
		})

		ginkgo.It("hits the maximum sleep time value in an exponential fashion and retries indefinitely", func() {
			gomega.Eventually(fakeSleeper.SleepCallCount, eventuallyTO).Should(gomega.Equal(500))
			gomega.Expect(fakeSleeper.SleepArgsForCall(25)).To(gomega.Equal(9539 * time.Millisecond))
			gomega.Expect(fakeSleeper.SleepArgsForCall(26)).To(gomega.Equal(10 * time.Second))
			gomega.Expect(fakeSleeper.SleepArgsForCall(27)).To(gomega.Equal(10 * time.Second))
			gomega.Expect(fakeSleeper.SleepArgsForCall(499)).To(gomega.Equal(10 * time.Second))
			gomega.Eventually(fakeDurationExceededHandler.DurationExceededHandlerCallCount, eventuallyTO).Should(
				gomega.Equal(120),
			)
		})
	})

	//nolint:dupl // 425-440 lines are duplicate of 285-300.
	ginkgo.When("an error occurs, then a block is successfully delivered", func() {
		ginkgo.BeforeEach(func() {
			fakeDeliverStreamer.DeliverReturnsOnCall(0, nil, fmt.Errorf("deliver-error"))
			fakeDeliverStreamer.DeliverReturnsOnCall(1, nil, fmt.Errorf("deliver-error"))
			fakeDeliverStreamer.DeliverReturnsOnCall(2, nil, fmt.Errorf("deliver-error"))
			fakeDeliverStreamer.DeliverReturnsOnCall(3, fakeDeliverClient, nil)
		})

		ginkgo.It("sleeps in an exponential fashion and retries until dial is successful", func() {
			gomega.Eventually(fakeDeliverStreamer.DeliverCallCount, eventuallyTO).Should(gomega.Equal(4))
			gomega.Expect(fakeSleeper.SleepCallCount()).To(gomega.Equal(3))
			gomega.Expect(fakeSleeper.SleepArgsForCall(0)).To(gomega.Equal(100 * time.Millisecond))
			gomega.Expect(fakeSleeper.SleepArgsForCall(1)).To(gomega.Equal(120 * time.Millisecond))
			gomega.Expect(fakeSleeper.SleepArgsForCall(2)).To(gomega.Equal(144 * time.Millisecond))
		})
	})

	ginkgo.It("sends a request to the deliver client for new blocks", func() {
		gomega.Eventually(fakeDeliverClient.SendCallCount, eventuallyTO).Should(gomega.Equal(1))
		mutex.Lock()
		defer mutex.Unlock()
		gomega.Expect(ccs).To(gomega.HaveLen(1))
	})

	ginkgo.When("the send fails", func() {
		ginkgo.BeforeEach(func() {
			fakeDeliverClient.SendReturnsOnCall(0, fmt.Errorf("fake-send-error"))
			fakeDeliverClient.SendReturnsOnCall(1, nil)
			fakeDeliverClient.CloseSendStub = nil
		})

		ginkgo.It("disconnects, sleeps and retries until the send is successful", func() {
			gomega.Eventually(fakeDeliverClient.SendCallCount, eventuallyTO).Should(gomega.Equal(2))
			gomega.Expect(fakeDeliverClient.CloseSendCallCount()).To(gomega.Equal(1))
			gomega.Expect(fakeSleeper.SleepCallCount()).To(gomega.Equal(1))
			gomega.Expect(fakeSleeper.SleepArgsForCall(0)).To(gomega.Equal(100 * time.Millisecond))
			mutex.Lock()
			defer mutex.Unlock()
			gomega.Expect(ccs).To(gomega.HaveLen(2))
			gomega.Eventually(ccs[0].GetState, eventuallyTO).Should(gomega.Equal(connectivity.Shutdown))
		})
	})

	ginkgo.It("attempts to read blocks from the deliver stream", func() {
		gomega.Eventually(fakeDeliverClient.RecvCallCount, eventuallyTO).Should(gomega.Equal(1))
	})

	ginkgo.When("reading blocks from the deliver stream fails", func() {
		ginkgo.BeforeEach(func() {
			// appease the race detector
			doneC := doneC
			recvStep := recvStep
			fakeDeliverClient := fakeDeliverClient

			fakeDeliverClient.CloseSendStub = nil
			fakeDeliverClient.RecvStub = func() (*orderer.DeliverResponse, error) {
				if fakeDeliverClient.RecvCallCount() == 1 {
					return nil, fmt.Errorf("fake-recv-error")
				}
				select {
				case <-recvStep:
					return nil, fmt.Errorf("fake-recv-step-error")
				case <-doneC:
					return nil, nil
				}
			}
		})

		ginkgo.It("disconnects, sleeps, and retries until the recv is successful", func() {
			gomega.Eventually(fakeDeliverClient.RecvCallCount, eventuallyTO).Should(gomega.Equal(2))
			gomega.Expect(fakeSleeper.SleepCallCount()).To(gomega.Equal(1))
			gomega.Expect(fakeSleeper.SleepArgsForCall(0)).To(gomega.Equal(100 * time.Millisecond))
		})
	})

	ginkgo.When("reading blocks from the deliver stream fails and then recovers", func() {
		ginkgo.BeforeEach(func() {
			// appease the race detector
			doneC := doneC
			recvStep := recvStep
			fakeDeliverClient := fakeDeliverClient

			fakeDeliverClient.CloseSendStub = func() error {
				if fakeDeliverClient.CloseSendCallCount() >= 5 {
					select {
					case <-doneC:
					case recvStep <- struct{}{}:
					}
				}
				return nil
			}
			fakeDeliverClient.RecvStub = func() (*orderer.DeliverResponse, error) {
				switch fakeDeliverClient.RecvCallCount() {
				case 1, 2, 4:
					return nil, fmt.Errorf("fake-recv-error")
				case 3:
					return &orderer.DeliverResponse{
						Type: &orderer.DeliverResponse_Block{
							Block: &common.Block{
								Header: &common.BlockHeader{
									Number: 8,
								},
							},
						},
					}, nil
				default:
					select {
					case <-recvStep:
						return nil, fmt.Errorf("fake-recv-step-error")
					case <-doneC:
						return nil, nil
					}
				}
			}
		})

		ginkgo.It("disconnects, sleeps, and retries until the recv is successful and resets the failure count", func() {
			gomega.Eventually(fakeDeliverClient.RecvCallCount, eventuallyTO).Should(gomega.Equal(5))
			gomega.Expect(fakeSleeper.SleepCallCount()).To(gomega.Equal(3))
			gomega.Expect(fakeSleeper.SleepArgsForCall(0)).To(gomega.Equal(100 * time.Millisecond))
			gomega.Expect(fakeSleeper.SleepArgsForCall(1)).To(gomega.Equal(120 * time.Millisecond))
			gomega.Expect(fakeSleeper.SleepArgsForCall(2)).To(gomega.Equal(100 * time.Millisecond))
		})
	})

	ginkgo.When("the deliver client returns a block", func() {
		ginkgo.BeforeEach(func() {
			// appease the race detector
			doneC := doneC
			recvStep := recvStep
			fakeDeliverClient := fakeDeliverClient

			fakeDeliverClient.RecvStub = func() (*orderer.DeliverResponse, error) {
				if fakeDeliverClient.RecvCallCount() == 1 {
					return &orderer.DeliverResponse{
						Type: &orderer.DeliverResponse_Block{
							Block: &common.Block{
								Header: &common.BlockHeader{
									Number: 8,
								},
							},
						},
					}, nil
				}
				select {
				case <-recvStep:
					return nil, fmt.Errorf("fake-recv-step-error")
				case <-doneC:
					return nil, nil
				}
			}
		})

		ginkgo.It("receives the block and loops, not sleeping", func() {
			gomega.Eventually(fakeDeliverClient.RecvCallCount, eventuallyTO).Should(gomega.Equal(2))
			gomega.Expect(fakeSleeper.SleepCallCount()).To(gomega.Equal(0))
		})

		ginkgo.It("checks the validity of the block", func() {
			gomega.Eventually(fakeUpdatableBlockVerifier.VerifyBlockCallCount, eventuallyTO).Should(gomega.Equal(1))
			block := fakeUpdatableBlockVerifier.VerifyBlockArgsForCall(0)
			gomega.Expect(block).To(test.ProtoEqual(&common.Block{
				Header: &common.BlockHeader{
					Number: 8,
				},
			}))
		})

		ginkgo.When("the block is invalid", func() {
			ginkgo.BeforeEach(func() {
				fakeUpdatableBlockVerifier.VerifyBlockReturns(fmt.Errorf("fake-verify-error"))
			})

			ginkgo.It("disconnects, sleeps, and tries again", func() {
				gomega.Eventually(fakeSleeper.SleepCallCount, eventuallyTO).Should(gomega.Equal(1))
				gomega.Expect(fakeDeliverClient.CloseSendCallCount()).To(gomega.Equal(1))
				mutex.Lock()
				defer mutex.Unlock()
				gomega.Expect(ccs).To(gomega.HaveLen(2))
			})
		})

		ginkgo.When("the block is valid", func() {
			ginkgo.It("handle the block", func() {
				gomega.Eventually(fakeBlockHandler.HandleBlockCallCount, eventuallyTO).Should(gomega.Equal(1))
				channelID, block := fakeBlockHandler.HandleBlockArgsForCall(0)
				gomega.Expect(channelID).To(gomega.Equal("channel-id"))
				gomega.Expect(block).To(gomega.Equal(&common.Block{
					Header: &common.BlockHeader{
						Number: 8,
					},
				},
				))
			})
		})

		ginkgo.When("handling the block fails", func() {
			ginkgo.BeforeEach(func() {
				fakeBlockHandler.HandleBlockReturns(fmt.Errorf("payload-error"))
			})

			ginkgo.It("disconnects, sleeps, and tries again", func() {
				gomega.Eventually(fakeSleeper.SleepCallCount, eventuallyTO).Should(gomega.Equal(1))
				gomega.Expect(fakeDeliverClient.CloseSendCallCount()).To(gomega.Equal(1))
				mutex.Lock()
				defer mutex.Unlock()
				gomega.Expect(ccs).To(gomega.HaveLen(2))
			})
		})
	})

	ginkgo.When("the deliver client returns a config block", func() {
		var env *common.Envelope

		ginkgo.BeforeEach(func() {
			// appease the race detector
			doneC := doneC
			recvStep := recvStep
			fakeDeliverClient := fakeDeliverClient
			env = &common.Envelope{
				Payload: protoutil.MarshalOrPanic(&common.Payload{
					Header: &common.Header{
						ChannelHeader: protoutil.MarshalOrPanic(&common.ChannelHeader{
							Type:      int32(common.HeaderType_CONFIG),
							ChannelId: "test-chain",
						}),
					},
					Data: protoutil.MarshalOrPanic(&common.ConfigEnvelope{
						Config: channelConfig, // it must be a legal config that can produce a new bundle
					}),
				}),
			}

			configBlock := &common.Block{
				Header: &common.BlockHeader{Number: 8},
				Data: &common.BlockData{
					Data: [][]byte{protoutil.MarshalOrPanic(env)},
				},
			}

			fakeDeliverClient.RecvStub = func() (*orderer.DeliverResponse, error) {
				if fakeDeliverClient.RecvCallCount() == 1 {
					return &orderer.DeliverResponse{
						Type: &orderer.DeliverResponse_Block{
							Block: configBlock,
						},
					}, nil
				}
				select {
				case <-recvStep:
					return nil, fmt.Errorf("fake-recv-step-error")
				case <-doneC:
					return nil, nil
				}
			}
		})

		ginkgo.It("receives the block and loops, not sleeping", func() {
			gomega.Eventually(fakeDeliverClient.RecvCallCount, eventuallyTO).Should(gomega.Equal(2))
			gomega.Expect(fakeSleeper.SleepCallCount()).To(gomega.Equal(0))
		})

		ginkgo.It("checks the validity of the block", func() {
			gomega.Eventually(fakeUpdatableBlockVerifier.VerifyBlockCallCount, eventuallyTO).Should(gomega.Equal(1))
			block := fakeUpdatableBlockVerifier.VerifyBlockArgsForCall(0)
			gomega.Expect(block).To(test.ProtoEqual(&common.Block{
				Header: &common.BlockHeader{Number: 8},
				Data: &common.BlockData{
					Data: [][]byte{protoutil.MarshalOrPanic(env)},
				},
			}))
		})

		ginkgo.It("handle the block and updates the verifier config", func() {
			gomega.Eventually(fakeBlockHandler.HandleBlockCallCount, eventuallyTO).Should(gomega.Equal(1))
			channelID, block := fakeBlockHandler.HandleBlockArgsForCall(0)
			gomega.Expect(channelID).To(gomega.Equal("channel-id"))
			gomega.Expect(block).To(test.ProtoEqual(&common.Block{
				Header: &common.BlockHeader{Number: 8},
				Data: &common.BlockData{
					Data: [][]byte{protoutil.MarshalOrPanic(env)},
				},
			},
			))
			gomega.Eventually(fakeUpdatableBlockVerifier.VerifyBlockCallCount, eventuallyTO).Should(gomega.Equal(1))
		})

		ginkgo.It("updates the orderer connection source", func() {
			gomega.Eventually(fakeOrdererConnectionSource.UpdateCallCount, eventuallyTO).Should(gomega.Equal(1))
			globalAddresses, orgsAddresses := fakeOrdererConnectionSource.UpdateArgsForCall(0)
			gomega.Expect(globalAddresses).To(gomega.BeNil())
			gomega.Expect(orgsAddresses).ToNot(gomega.BeNil())
			gomega.Expect(orgsAddresses).To(gomega.HaveLen(1))
			orgAddr, ok := orgsAddresses["SampleOrg"]
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(orgAddr.Addresses).To(gomega.Equal([]string{
				"127.0.0.1:7050", "127.0.0.1:7051", "127.0.0.1:7052",
			}))
			gomega.Expect(orgAddr.RootCerts).To(gomega.HaveLen(2))
		})
	})

	ginkgo.When("the deliver client returns a status", func() {
		var status common.Status

		ginkgo.BeforeEach(func() {
			// appease the race detector
			doneC := doneC
			recvStep := recvStep
			fakeDeliverClient := fakeDeliverClient

			status = common.Status_SUCCESS
			fakeDeliverClient.RecvStub = func() (*orderer.DeliverResponse, error) {
				if fakeDeliverClient.RecvCallCount() == 1 {
					return &orderer.DeliverResponse{
						Type: &orderer.DeliverResponse_Status{
							Status: status,
						},
					}, nil
				}
				select {
				case <-recvStep:
					return nil, fmt.Errorf("fake-recv-step-error")
				case <-doneC:
					return nil, nil
				}
			}
		})

		ginkgo.It("disconnects with an error, and sleeps because the block request is infinite and "+
			"should never complete", func() {
			gomega.Eventually(fakeSleeper.SleepCallCount, eventuallyTO).Should(gomega.Equal(1))
		})

		ginkgo.When("the status is not successful", func() {
			ginkgo.BeforeEach(func() {
				status = common.Status_FORBIDDEN
			})

			ginkgo.It("still disconnects with an error", func() {
				gomega.Eventually(fakeSleeper.SleepCallCount, eventuallyTO).Should(gomega.Equal(1))
			})
		})
	})
})

func testSetup(certDir string, consensusClass string) (*common.Config, bccsp.BCCSP, error) {
	var configProfile *configtxgen.Profile
	tlsCA, err := tlsgen.NewCA()
	if err != nil {
		return nil, nil, err
	}

	switch consensusClass {
	case "CFT":
		configProfile = configtxgen.Load(configtxgen.SampleAppChannelEtcdRaftProfile, arma_testutil.GetDevConfigDir())
		err = generateCertificates(configProfile, tlsCA, certDir)
	case "BFT":
		configProfile = configtxgen.Load(configtxgen.SampleAppChannelSmartBftProfile, arma_testutil.GetDevConfigDir())
		err = generateCertificatesSmartBFT(configProfile, tlsCA, certDir)
	default:
		err = fmt.Errorf("expected CFT or BFT")
	}

	if err != nil {
		return nil, nil, err
	}

	bootstrapper, err := configtxgen.NewBootstrapper(configProfile)
	if err != nil {
		return nil, nil, err
	}
	channelConfigProto := &common.Config{ChannelGroup: bootstrapper.GenesisChannelGroup()}
	cryptoProvider, err := sw.NewDefaultSecurityLevelWithKeystore(sw.NewDummyKeyStore())
	if err != nil {
		return nil, nil, err
	}

	return channelConfigProto, cryptoProvider, nil
}

// TODO this pattern repeats itself in several places. Make it common in the 'genesisconfig' package to easily create
// Raft genesis blocks
func generateCertificates(confAppRaft *configtxgen.Profile, tlsCA tlsgen.CA, certDir string) error {
	for i, c := range confAppRaft.Orderer.EtcdRaft.Consenters {
		srvC, err := tlsCA.NewServerCertKeyPair(c.Host)
		if err != nil {
			return err
		}
		srvP := path.Join(certDir, fmt.Sprintf("server%d.crt", i))
		err = os.WriteFile(srvP, srvC.Cert, 0o644)
		if err != nil {
			return err
		}

		clnC, err := tlsCA.NewClientCertKeyPair()
		if err != nil {
			return err
		}
		clnP := path.Join(certDir, fmt.Sprintf("client%d.crt", i))
		err = os.WriteFile(clnP, clnC.Cert, 0o644)
		if err != nil {
			return err
		}

		c.ServerTlsCert = []byte(srvP)
		c.ClientTlsCert = []byte(clnP)
	}

	return nil
}

func generateCertificatesSmartBFT(confAppSmartBFT *configtxgen.Profile, tlsCA tlsgen.CA, certDir string) error {
	for i, c := range confAppSmartBFT.Orderer.ConsenterMapping {
		srvC, err := tlsCA.NewServerCertKeyPair(c.Host)
		if err != nil {
			return err
		}

		srvP := path.Join(certDir, fmt.Sprintf("server%d.crt", i))
		err = os.WriteFile(srvP, srvC.Cert, 0o644)
		if err != nil {
			return err
		}

		clnC, err := tlsCA.NewClientCertKeyPair()
		if err != nil {
			return err
		}

		clnP := path.Join(certDir, fmt.Sprintf("client%d.crt", i))
		err = os.WriteFile(clnP, clnC.Cert, 0o644)
		if err != nil {
			return err
		}

		c.Identity = srvP
		c.ServerTLSCert = srvP
		c.ClientTLSCert = clnP
	}

	return nil
}
