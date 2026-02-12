/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliver_test

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"io"
	"time"

	"github.com/hyperledger/fabric-x-common/tools/test"

	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-lib-go/common/metrics/metricsfakes"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/hyperledger/fabric-x-common/common/crypto/tlsgen"
	"github.com/hyperledger/fabric-x-common/common/deliver"
	"github.com/hyperledger/fabric-x-common/common/deliver/mock"
	"github.com/hyperledger/fabric-x-common/common/ledger/blockledger"
	"github.com/hyperledger/fabric-x-common/common/util"
	"github.com/hyperledger/fabric-x-common/protoutil"
)

var (
	seekOldest = &ab.SeekPosition{
		Type: &ab.SeekPosition_Oldest{Oldest: &ab.SeekOldest{}},
	}

	seekNewest = &ab.SeekPosition{
		Type: &ab.SeekPosition_Newest{Newest: &ab.SeekNewest{}},
	}
)

var _ = ginkgo.Describe("Deliver", func() {
	ginkgo.Describe("NewHandler", func() {
		var fakeChainManager *mock.ChainManager
		var cert *x509.Certificate
		var certBytes []byte
		ginkgo.BeforeEach(func() {
			fakeChainManager = &mock.ChainManager{}

			ca, err := tlsgen.NewCA()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			certBytes = ca.CertBytes()

			der, _ := pem.Decode(ca.CertBytes())
			cert, err = x509.ParseCertificate(der.Bytes)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("returns a new handler", func() {
			handler := deliver.NewHandler(
				fakeChainManager,
				time.Second,
				false,
				deliver.NewMetrics(&disabled.Provider{}),
				false)
			gomega.Expect(handler).NotTo(gomega.BeNil())

			gomega.Expect(handler.ChainManager).To(gomega.Equal(fakeChainManager))
			gomega.Expect(handler.TimeWindow).To(gomega.Equal(time.Second))
			// binding inspector is func; unable to easily validate
			gomega.Expect(handler.BindingInspector).NotTo(gomega.BeNil())
		})

		ginkgo.Context("Handler is initialized with expiration checks", func() {
			ginkgo.It("Returns exactly what is found in the certificate", func() {
				handler := deliver.NewHandler(
					fakeChainManager,
					time.Second,
					false,
					deliver.NewMetrics(&disabled.Provider{}),
					false)

				gomega.Expect(handler.ExpirationCheckFunc(certBytes)).To(gomega.Equal(cert.NotAfter))
			})
		})

		ginkgo.Context("Handler is initialized without expiration checks", func() {
			ginkgo.It("Doesn't parse the NotAfter time of the certificate", func() {
				handler := deliver.NewHandler(
					fakeChainManager,
					time.Second,
					false,
					deliver.NewMetrics(&disabled.Provider{}),
					true)

				gomega.Expect(handler.ExpirationCheckFunc(certBytes)).NotTo(gomega.Equal(cert.NotAfter))
			})
		})
	})

	ginkgo.Describe("ExtractChannelHeaderCertHash", func() {
		ginkgo.It("extracts the TLS certificate hash from a channel header", func() {
			chdr := &cb.ChannelHeader{TlsCertHash: []byte("tls-cert")}

			result := deliver.ExtractChannelHeaderCertHash(chdr)
			gomega.Expect(result).To(gomega.Equal([]byte("tls-cert")))
		})

		ginkgo.Context("when the message is not a channel header", func() {
			ginkgo.It("returns nil", func() {
				result := deliver.ExtractChannelHeaderCertHash(&cb.Envelope{})
				gomega.Expect(result).To(gomega.BeNil())
			})
		})

		ginkgo.Context("when the message is nil", func() {
			ginkgo.It("returns nil", func() {
				var ch *cb.ChannelHeader
				result := deliver.ExtractChannelHeaderCertHash(ch)
				gomega.Expect(result).To(gomega.BeNil())
			})
		})
	})

	ginkgo.Describe("Handle", func() {
		var (
			errCh                 chan struct{}
			fakeChain             *mock.Chain
			fakeBlockReader       *mock.BlockReader
			fakeBlockIterator     *mock.BlockIterator
			fakeChainManager      *mock.ChainManager
			fakePolicyChecker     *mock.PolicyChecker
			fakeReceiver          *mock.Receiver
			fakeResponseSender    *mock.ResponseSender
			fakeInspector         *mock.Inspector
			fakeStreamsOpened     *metricsfakes.Counter
			fakeStreamsClosed     *metricsfakes.Counter
			fakeRequestsReceived  *metricsfakes.Counter
			fakeRequestsCompleted *metricsfakes.Counter
			fakeBlocksSent        *metricsfakes.Counter

			handler *deliver.Handler
			server  *deliver.Server

			channelHeader *cb.ChannelHeader
			seekInfo      *ab.SeekInfo
			ts            *timestamppb.Timestamp

			channelHeaderPayload []byte
			seekInfoPayload      []byte
			envelope             *cb.Envelope
		)

		ginkgo.BeforeEach(func() {
			errCh = make(chan struct{})
			fakeChain = &mock.Chain{}
			fakeChain.ErroredReturns(errCh)

			block := &cb.Block{
				Header: &cb.BlockHeader{Number: 100},
			}
			fakeBlockIterator = &mock.BlockIterator{}
			fakeBlockIterator.NextReturns(block, cb.Status_SUCCESS)

			fakeBlockReader = &mock.BlockReader{}
			fakeBlockReader.HeightReturns(1000)
			fakeBlockReader.IteratorReturns(fakeBlockIterator, 100)
			fakeChain.ReaderReturns(fakeBlockReader)

			fakeChainManager = &mock.ChainManager{}
			fakeChainManager.GetChainReturns(fakeChain)

			fakePolicyChecker = &mock.PolicyChecker{}
			fakeReceiver = &mock.Receiver{}
			fakeResponseSender = &mock.ResponseSender{}
			fakeResponseSender.DataTypeReturns("block")

			fakeInspector = &mock.Inspector{}

			fakeStreamsOpened = &metricsfakes.Counter{}
			fakeStreamsOpened.WithReturns(fakeStreamsOpened)
			fakeStreamsClosed = &metricsfakes.Counter{}
			fakeStreamsClosed.WithReturns(fakeStreamsClosed)
			fakeRequestsReceived = &metricsfakes.Counter{}
			fakeRequestsReceived.WithReturns(fakeRequestsReceived)
			fakeRequestsCompleted = &metricsfakes.Counter{}
			fakeRequestsCompleted.WithReturns(fakeRequestsCompleted)
			fakeBlocksSent = &metricsfakes.Counter{}
			fakeBlocksSent.WithReturns(fakeBlocksSent)

			deliverMetrics := &deliver.Metrics{
				StreamsOpened:     fakeStreamsOpened,
				StreamsClosed:     fakeStreamsClosed,
				RequestsReceived:  fakeRequestsReceived,
				RequestsCompleted: fakeRequestsCompleted,
				BlocksSent:        fakeBlocksSent,
			}

			handler = &deliver.Handler{
				ChainManager:     fakeChainManager,
				TimeWindow:       time.Second,
				BindingInspector: fakeInspector,
				Metrics:          deliverMetrics,
				ExpirationCheckFunc: func([]byte) time.Time {
					return time.Time{}
				},
			}
			server = &deliver.Server{
				Receiver:       fakeReceiver,
				PolicyChecker:  fakePolicyChecker,
				ResponseSender: fakeResponseSender,
			}

			ts = util.CreateUtcTimestamp()
			channelHeader = &cb.ChannelHeader{
				ChannelId: "chain-id",
				Timestamp: ts,
			}
			seekInfo = &ab.SeekInfo{
				Start: &ab.SeekPosition{
					Type: &ab.SeekPosition_Specified{
						Specified: &ab.SeekSpecified{Number: 100},
					},
				},
				Stop: &ab.SeekPosition{
					Type: &ab.SeekPosition_Specified{
						Specified: &ab.SeekSpecified{Number: 100},
					},
				},
			}

			channelHeaderPayload = nil
			seekInfoPayload = nil

			envelope = &cb.Envelope{}
			fakeReceiver.RecvReturns(envelope, nil)
			fakeReceiver.RecvReturnsOnCall(1, nil, io.EOF)
		})

		ginkgo.JustBeforeEach(func() {
			if channelHeaderPayload == nil {
				channelHeaderPayload = protoutil.MarshalOrPanic(channelHeader)
			}
			if seekInfoPayload == nil {
				seekInfoPayload = protoutil.MarshalOrPanic(seekInfo)
			}
			if envelope.Payload == nil {
				payload := &cb.Payload{
					Header: &cb.Header{
						ChannelHeader:   channelHeaderPayload,
						SignatureHeader: protoutil.MarshalOrPanic(&cb.SignatureHeader{}),
					},
					Data: seekInfoPayload,
				}
				envelope.Payload = protoutil.MarshalOrPanic(payload)
			}
		})

		ginkgo.It("records streams opened before streams closed", func() {
			fakeStreamsOpened.AddStub = func(delta float64) {
				defer ginkgo.GinkgoRecover()
				gomega.Expect(fakeStreamsClosed.AddCallCount()).To(gomega.Equal(0))
			}

			err := handler.Handle(context.Background(), server)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Expect(fakeStreamsOpened.AddCallCount()).To(gomega.Equal(1))
			gomega.Expect(fakeStreamsOpened.AddArgsForCall(0)).To(gomega.BeNumerically("~", 1.0))
		})

		ginkgo.It("records streams closed after streams opened", func() {
			fakeStreamsClosed.AddStub = func(delta float64) {
				defer ginkgo.GinkgoRecover()
				gomega.Expect(fakeStreamsOpened.AddCallCount()).To(gomega.Equal(1))
			}

			err := handler.Handle(context.Background(), server)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Expect(fakeStreamsClosed.AddCallCount()).To(gomega.Equal(1))
			gomega.Expect(fakeStreamsClosed.AddArgsForCall(0)).To(gomega.BeNumerically("~", 1.0))
		})

		ginkgo.It("validates the channel header with the binding inspector", func() {
			err := handler.Handle(context.Background(), server)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Expect(fakeInspector.InspectCallCount()).To(gomega.Equal(1))
			ctx, header := fakeInspector.InspectArgsForCall(0)
			gomega.Expect(ctx).To(gomega.Equal(context.Background()))
			gomega.Expect(header).To(test.ProtoEqual(channelHeader))
		})

		ginkgo.Context("when channel header validation fails", func() {
			ginkgo.BeforeEach(func() {
				fakeInspector.InspectReturns(errors.New("bad-header-thingy"))
			})

			ginkgo.It("sends a bad request message", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_BAD_REQUEST))
			})
		})

		ginkgo.It("gets the chain from the chain manager", func() {
			err := handler.Handle(context.Background(), server)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Expect(fakeChainManager.GetChainCallCount()).To(gomega.Equal(1))
			chid := fakeChainManager.GetChainArgsForCall(0)
			gomega.Expect(chid).To(gomega.Equal("chain-id"))
		})

		ginkgo.It("receives messages until io.EOF is returned", func() {
			err := handler.Handle(context.Background(), server)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Expect(fakeReceiver.RecvCallCount()).To(gomega.Equal(2))
		})

		ginkgo.It("evaluates access control", func() {
			err := handler.Handle(context.Background(), server)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Expect(fakePolicyChecker.CheckPolicyCallCount()).To(gomega.BeNumerically(">=", 1))
			e, cid := fakePolicyChecker.CheckPolicyArgsForCall(0)
			gomega.Expect(e).To(test.ProtoEqual(envelope))
			gomega.Expect(cid).To(gomega.Equal("chain-id"))
		})

		ginkgo.It("gets a block iterator from the starting block", func() {
			err := handler.Handle(context.Background(), server)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Expect(fakeBlockReader.IteratorCallCount()).To(gomega.Equal(1))
			startPosition := fakeBlockReader.IteratorArgsForCall(0)
			gomega.Expect(startPosition).To(test.ProtoEqual(seekInfo.Start))
		})

		ginkgo.Context("when multiple blocks are requested", func() {
			ginkgo.BeforeEach(func() {
				fakeBlockIterator.NextStub = func() (*cb.Block, cb.Status) {
					blk := &cb.Block{
						Header: &cb.BlockHeader{Number: 994 + uint64(fakeBlockIterator.NextCallCount())},
					}
					return blk, cb.Status_SUCCESS
				}
				seekInfo = &ab.SeekInfo{
					Start: &ab.SeekPosition{
						Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: 995}},
					},
					Stop: seekNewest,
				}
			})

			ginkgo.It("sends all requested blocks", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.SendBlockResponseCallCount()).To(gomega.Equal(5))
				for i := 0; i < 5; i++ {
					b, _, _, _ := fakeResponseSender.SendBlockResponseArgsForCall(i)
					gomega.Expect(b).To(gomega.Equal(&cb.Block{
						Header: &cb.BlockHeader{Number: 995 + uint64(i)},
					}))
				}
			})

			ginkgo.It("records requests received, blocks sent, and requests completed", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeRequestsReceived.AddCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeRequestsReceived.AddArgsForCall(0)).To(gomega.BeNumerically("~", 1.0))
				gomega.Expect(fakeRequestsReceived.WithCallCount()).To(gomega.Equal(1))
				labelValues := fakeRequestsReceived.WithArgsForCall(0)
				gomega.Expect(labelValues).To(gomega.Equal([]string{
					"channel", "chain-id",
					"filtered", "false",
					"data_type", "block",
				}))

				gomega.Expect(fakeBlocksSent.AddCallCount()).To(gomega.Equal(5))
				gomega.Expect(fakeBlocksSent.WithCallCount()).To(gomega.Equal(5))
				for i := 0; i < 5; i++ {
					gomega.Expect(fakeBlocksSent.AddArgsForCall(i)).To(gomega.BeNumerically("~", 1.0))
					labelValues := fakeBlocksSent.WithArgsForCall(i)
					gomega.Expect(labelValues).To(gomega.Equal([]string{
						"channel", "chain-id",
						"filtered", "false",
						"data_type", "block",
					}))
				}

				gomega.Expect(fakeRequestsCompleted.AddCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeRequestsCompleted.AddArgsForCall(0)).To(gomega.BeNumerically("~", 1.0))
				gomega.Expect(fakeRequestsCompleted.WithCallCount()).To(gomega.Equal(1))
				labelValues = fakeRequestsCompleted.WithArgsForCall(0)
				gomega.Expect(labelValues).To(gomega.Equal([]string{
					"channel", "chain-id",
					"filtered", "false",
					"data_type", "block",
					"success", "true",
				}))
			})
		})

		ginkgo.Context("when seek info is configured to stop at the oldest block", func() {
			ginkgo.BeforeEach(func() {
				seekInfo = &ab.SeekInfo{Start: &ab.SeekPosition{}, Stop: seekOldest}
			})

			ginkgo.It("sends only the first block returned by the iterator", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeBlockReader.IteratorCallCount()).To(gomega.Equal(1))
				start := fakeBlockReader.IteratorArgsForCall(0)
				gomega.Expect(start).To(test.ProtoEqual(&ab.SeekPosition{}))
				gomega.Expect(fakeBlockIterator.NextCallCount()).To(gomega.Equal(1))

				gomega.Expect(fakeResponseSender.SendBlockResponseCallCount()).To(gomega.Equal(1))
				b, _, _, _ := fakeResponseSender.SendBlockResponseArgsForCall(0)
				gomega.Expect(b).To(test.ProtoEqual(&cb.Block{
					Header: &cb.BlockHeader{Number: 100},
				}))
			})
		})

		ginkgo.Context("when seek info is configured to stop at the newest block", func() {
			ginkgo.BeforeEach(func() {
				seekInfo = &ab.SeekInfo{Start: &ab.SeekPosition{}, Stop: seekNewest}

				fakeBlockReader.HeightReturns(3)
				fakeBlockIterator.NextStub = func() (*cb.Block, cb.Status) {
					blk := &cb.Block{
						Header: &cb.BlockHeader{Number: uint64(fakeBlockIterator.NextCallCount())},
					}
					return blk, cb.Status_SUCCESS
				}
			})

			ginkgo.It("sends blocks until the iterator reaches the reader height", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeBlockReader.IteratorCallCount()).To(gomega.Equal(1))
				start := fakeBlockReader.IteratorArgsForCall(0)
				gomega.Expect(start).To(test.ProtoEqual(&ab.SeekPosition{}))

				gomega.Expect(fakeBlockIterator.NextCallCount()).To(gomega.Equal(2))
				gomega.Expect(fakeResponseSender.SendBlockResponseCallCount()).To(gomega.Equal(2))
				for i := 0; i < fakeResponseSender.SendBlockResponseCallCount(); i++ {
					b, _, _, _ := fakeResponseSender.SendBlockResponseArgsForCall(i)
					gomega.Expect(b).To(test.ProtoEqual(&cb.Block{
						Header: &cb.BlockHeader{Number: uint64(i + 1)},
					}))
				}
			})
		})

		ginkgo.Context("when seek info is configured to send just the newest block and a new block is"+
			" committed to the ledger after the iterator is acquired", func() {
			ginkgo.BeforeEach(func() {
				seekInfo = &ab.SeekInfo{Start: seekNewest, Stop: seekNewest}

				fakeBlockReader.IteratorReturns(fakeBlockIterator, 0)
				fakeBlockReader.HeightReturns(2)
				fakeChain.ReaderReturns(fakeBlockReader)
				fakeBlockIterator.NextStub = func() (*cb.Block, cb.Status) {
					blk := &cb.Block{
						Header: &cb.BlockHeader{Number: uint64(fakeBlockIterator.NextCallCount() - 1)},
					}
					return blk, cb.Status_SUCCESS
				}
			})

			ginkgo.It("sends only the newest block at the time the iterator was acquired", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeBlockReader.IteratorCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeBlockIterator.NextCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeResponseSender.SendBlockResponseCallCount()).To(gomega.Equal(1))
				for i := 0; i < fakeResponseSender.SendBlockResponseCallCount(); i++ {
					b, _, _, _ := fakeResponseSender.SendBlockResponseArgsForCall(i)
					gomega.Expect(b).To(test.ProtoEqual(&cb.Block{
						Header: &cb.BlockHeader{Number: uint64(i)},
					}))
				}
			})
		})

		ginkgo.Context("when seek info is configured to header with sig content type", func() {
			var cachedBlocks []*cb.Block
			ginkgo.BeforeEach(func() {
				seekInfo = &ab.SeekInfo{
					Start:       &ab.SeekPosition{},
					Stop:        seekNewest,
					ContentType: ab.SeekInfo_HEADER_WITH_SIG,
				}

				fakeBlockReader.HeightReturns(3)
				fakeBlockIterator.NextStub = func() (*cb.Block, cb.Status) {
					blk := &cb.Block{
						Header:   &cb.BlockHeader{Number: uint64(fakeBlockIterator.NextCallCount())},
						Data:     &cb.BlockData{Data: [][]byte{{1}, {2}}},
						Metadata: &cb.BlockMetadata{Metadata: [][]byte{{3}, {4}}},
					}
					cachedBlocks = append(cachedBlocks, blk)
					return blk, cb.Status_SUCCESS
				}
			})

			ginkgo.It("sends blocks with nil Data, but does not mutate cached blocks", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeBlockReader.IteratorCallCount()).To(gomega.Equal(1))
				start := fakeBlockReader.IteratorArgsForCall(0)
				gomega.Expect(start).To(test.ProtoEqual(&ab.SeekPosition{}))

				gomega.Expect(fakeBlockIterator.NextCallCount()).To(gomega.Equal(2))
				gomega.Expect(fakeResponseSender.SendBlockResponseCallCount()).To(gomega.Equal(2))
				for i := 0; i < fakeResponseSender.SendBlockResponseCallCount(); i++ {
					b, _, _, _ := fakeResponseSender.SendBlockResponseArgsForCall(i)
					gomega.Expect(b).To(test.ProtoEqual(&cb.Block{
						Header:   &cb.BlockHeader{Number: uint64(i + 1)},
						Data:     nil,
						Metadata: &cb.BlockMetadata{Metadata: [][]byte{{3}, {4}}},
					}))
				}

				for _, b := range cachedBlocks {
					gomega.Expect(b.Data).ToNot(gomega.BeNil())
				}
			})
		})

		ginkgo.Context(
			"when seek info is configured to header with sig content type and block can be a config block",
			func() {
				ginkgo.BeforeEach(func() {
					seekInfo = &ab.SeekInfo{
						Start:       &ab.SeekPosition{},
						Stop:        seekNewest,
						ContentType: ab.SeekInfo_HEADER_WITH_SIG,
					}
					fakeBlockReader.HeightReturns(4)
					fakeBlockIterator.NextStub = func() (*cb.Block, cb.Status) {
						nxtCallCount := fakeBlockIterator.NextCallCount()
						block := &cb.Block{
							Header:   &cb.BlockHeader{Number: uint64(nxtCallCount)},
							Metadata: &cb.BlockMetadata{Metadata: [][]byte{{3}, {4}}},
						}
						if nxtCallCount == 1 || nxtCallCount == 3 {
							block.Data = &cb.BlockData{Data: [][]byte{{1}, {2}}}
						} else {
							channelHeader = protoutil.MakeChannelHeader(cb.HeaderType_CONFIG, int32(1), "chain-1", 0)
							payloadSignatureHeader := protoutil.MakeSignatureHeader(nil, make([]byte, 24))
							protoutil.SetTxID(channelHeader, payloadSignatureHeader)
							payloadHeader := protoutil.MakePayloadHeader(channelHeader, payloadSignatureHeader)
							cg := protoutil.NewConfigGroup()
							payload := &cb.Payload{
								Header: payloadHeader,
								Data: protoutil.MarshalOrPanic(&cb.ConfigEnvelope{
									Config: &cb.Config{ChannelGroup: cg},
								}),
							}

							block.Data = &cb.BlockData{
								Data: [][]byte{protoutil.MarshalOrPanic(&cb.Envelope{
									Payload:   protoutil.MarshalOrPanic(payload),
									Signature: nil,
								})},
							}
							block.Header.DataHash = nil
						}
						return block, cb.Status_SUCCESS
					}
				})

				ginkgo.It("sends blocks with non nil Data", func() {
					err := handler.Handle(context.Background(), server)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(fakeBlockReader.IteratorCallCount()).To(gomega.Equal(1))
					start := fakeBlockReader.IteratorArgsForCall(0)
					gomega.Expect(start).To(test.ProtoEqual(&ab.SeekPosition{}))
					gomega.Expect(fakeBlockIterator.NextCallCount()).To(gomega.Equal(3))
					gomega.Expect(fakeResponseSender.SendBlockResponseCallCount()).To(gomega.Equal(3))
					for i := range fakeResponseSender.SendBlockResponseCallCount() {
						b, _, _, _ := fakeResponseSender.SendBlockResponseArgsForCall(i)
						if i+1 == 1 || i+1 == 3 {
							gomega.Expect(b).To(test.ProtoEqual(&cb.Block{
								Header:   &cb.BlockHeader{Number: uint64(i + 1)},
								Data:     nil,
								Metadata: &cb.BlockMetadata{Metadata: [][]byte{{3}, {4}}},
							}))
						} else {
							payloadSignatureHeader := protoutil.MakeSignatureHeader(nil, make([]byte, 24))
							protoutil.SetTxID(channelHeader, payloadSignatureHeader)
							payloadHeader := protoutil.MakePayloadHeader(channelHeader, payloadSignatureHeader)
							cg := protoutil.NewConfigGroup()
							payload := &cb.Payload{
								Header: payloadHeader,
								Data: protoutil.MarshalOrPanic(&cb.ConfigEnvelope{
									Config: &cb.Config{ChannelGroup: cg},
								}),
							}
							blk := &cb.Block{
								Header: &cb.BlockHeader{Number: uint64(i + 1)},
								Data: &cb.BlockData{
									Data: [][]byte{protoutil.MarshalOrPanic(&cb.Envelope{
										Payload:   protoutil.MarshalOrPanic(payload),
										Signature: nil,
									})},
								},
								Metadata: &cb.BlockMetadata{Metadata: [][]byte{{3}, {4}}},
							}
							gomega.Expect(b).To(test.ProtoEqual(blk))
							gomega.Expect(b.Data.Data).NotTo(gomega.BeNil())
						}
					}
				})
			},
		)

		ginkgo.Context("when filtered blocks are requested", func() {
			var fakeResponseSender *mock.FilteredResponseSender

			ginkgo.BeforeEach(func() {
				fakeResponseSender = &mock.FilteredResponseSender{}
				fakeResponseSender.IsFilteredReturns(true)
				fakeResponseSender.DataTypeReturns("filtered_block")
				server.ResponseSender = fakeResponseSender
			})

			ginkgo.It("checks if the response sender is filtered", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.IsFilteredCallCount()).To(gomega.Equal(1))
			})

			ginkgo.Context("when the response sender indicates it is not filtered", func() {
				ginkgo.BeforeEach(func() {
					fakeResponseSender.IsFilteredReturns(false)
				})

				ginkgo.It("labels the metric with filtered=false", func() {
					err := handler.Handle(context.Background(), server)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					gomega.Expect(fakeRequestsReceived.WithCallCount()).To(gomega.Equal(1))
					labelValues := fakeRequestsReceived.WithArgsForCall(0)
					gomega.Expect(labelValues).To(gomega.Equal([]string{
						"channel", "chain-id",
						"filtered", "false",
						"data_type", "filtered_block",
					}))
				})
			})

			//nolint:dupl // 655-690 lines are duplicate of 713-748.
			ginkgo.It("records requests received, blocks sent, and requests completed with the filtered label "+
				"set to true", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeRequestsReceived.AddCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeRequestsReceived.AddArgsForCall(0)).To(gomega.BeNumerically("~", 1.0))
				gomega.Expect(fakeRequestsReceived.WithCallCount()).To(gomega.Equal(1))
				labelValues := fakeRequestsReceived.WithArgsForCall(0)
				gomega.Expect(labelValues).To(gomega.Equal([]string{
					"channel", "chain-id",
					"filtered", "true",
					"data_type", "filtered_block",
				}))

				gomega.Expect(fakeBlocksSent.AddCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeBlocksSent.WithCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeBlocksSent.AddArgsForCall(0)).To(gomega.BeNumerically("~", 1.0))
				labelValues = fakeBlocksSent.WithArgsForCall(0)
				gomega.Expect(labelValues).To(gomega.Equal([]string{
					"channel", "chain-id",
					"filtered", "true",
					"data_type", "filtered_block",
				}))

				gomega.Expect(fakeRequestsCompleted.AddCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeRequestsCompleted.AddArgsForCall(0)).To(gomega.BeNumerically("~", 1.0))
				gomega.Expect(fakeRequestsCompleted.WithCallCount()).To(gomega.Equal(1))
				labelValues = fakeRequestsCompleted.WithArgsForCall(0)
				gomega.Expect(labelValues).To(gomega.Equal([]string{
					"channel", "chain-id",
					"filtered", "true",
					"data_type", "filtered_block",
					"success", "true",
				}))
			})
		})

		ginkgo.Context("when blocks with private data are requested", func() {
			var fakeResponseSender *mock.PrivateDataResponseSender

			ginkgo.BeforeEach(func() {
				fakeResponseSender = &mock.PrivateDataResponseSender{}
				fakeResponseSender.DataTypeReturns("block_and_pvtdata")
				server.ResponseSender = fakeResponseSender
			})

			ginkgo.It("handles the request and returns private data for all collections", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(fakeResponseSender.DataTypeCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeResponseSender.SendBlockResponseCallCount()).To(gomega.Equal(1))
				b, _, _, _ := fakeResponseSender.SendBlockResponseArgsForCall(0)
				gomega.Expect(b).To(test.ProtoEqual(&cb.Block{
					Header: &cb.BlockHeader{Number: 100},
				}))
			})

			//nolint:dupl // 655-690 lines are duplicate of 713-748.
			ginkgo.It("records requests received, blocks sent, and requests completed with the privatedata "+
				"label set to true", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeRequestsReceived.AddCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeRequestsReceived.AddArgsForCall(0)).To(gomega.BeNumerically("~", 1.0))
				gomega.Expect(fakeRequestsReceived.WithCallCount()).To(gomega.Equal(1))
				labelValues := fakeRequestsReceived.WithArgsForCall(0)
				gomega.Expect(labelValues).To(gomega.Equal([]string{
					"channel", "chain-id",
					"filtered", "false",
					"data_type", "block_and_pvtdata",
				}))

				gomega.Expect(fakeBlocksSent.AddCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeBlocksSent.WithCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeBlocksSent.AddArgsForCall(0)).To(gomega.BeNumerically("~", 1.0))
				labelValues = fakeBlocksSent.WithArgsForCall(0)
				gomega.Expect(labelValues).To(gomega.Equal([]string{
					"channel", "chain-id",
					"filtered", "false",
					"data_type", "block_and_pvtdata",
				}))

				gomega.Expect(fakeRequestsCompleted.AddCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeRequestsCompleted.AddArgsForCall(0)).To(gomega.BeNumerically("~", 1.0))
				gomega.Expect(fakeRequestsCompleted.WithCallCount()).To(gomega.Equal(1))
				labelValues = fakeRequestsCompleted.WithArgsForCall(0)
				gomega.Expect(labelValues).To(gomega.Equal([]string{
					"channel", "chain-id",
					"filtered", "false",
					"data_type", "block_and_pvtdata",
					"success", "true",
				}))
			})
		})

		ginkgo.Context("when sending the block fails", func() {
			ginkgo.BeforeEach(func() {
				fakeResponseSender.SendBlockResponseReturns(errors.New("send-fails"))
			})

			ginkgo.It("returns the error", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).To(gomega.MatchError("send-fails"))
			})
		})

		ginkgo.It("sends a success response", func() {
			err := handler.Handle(context.Background(), server)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
			resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
			gomega.Expect(resp).To(gomega.Equal(cb.Status_SUCCESS))
		})

		ginkgo.It("HandleAttestation sends requested block", func() {
			err := handler.HandleAttestation(context.Background(), server, envelope)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(fakeResponseSender.SendBlockResponseCallCount()).To(gomega.Equal(1))
			b, _, _, _ := fakeResponseSender.SendBlockResponseArgsForCall(0)
			gomega.Expect(b).To(test.ProtoEqual(&cb.Block{
				Header: &cb.BlockHeader{Number: 100},
			}))
		})

		ginkgo.Context("when sending the success status fails", func() {
			ginkgo.BeforeEach(func() {
				fakeResponseSender.SendStatusResponseReturns(errors.New("send-success-fails"))
			})

			ginkgo.It("returns the error", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).To(gomega.MatchError("send-success-fails"))
			})

			ginkgo.It("HandleAttestation returns the error", func() {
				err := handler.HandleAttestation(context.Background(), server, envelope)
				gomega.Expect(err).To(gomega.MatchError("send-success-fails"))
			})
		})

		ginkgo.Context("when receive fails", func() {
			ginkgo.BeforeEach(func() {
				fakeReceiver.RecvReturns(nil, errors.New("oh bother"))
			})

			ginkgo.It("returns the error", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).To(gomega.MatchError("oh bother"))
			})
		})

		ginkgo.Context("when unmarshalling the payload fails", func() {
			ginkgo.BeforeEach(func() {
				envelope.Payload = []byte("completely-bogus-data")
			})

			ginkgo.It("sends a bad request message", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_BAD_REQUEST))
			})
		})

		ginkgo.Context("when the payload header is nil", func() {
			ginkgo.BeforeEach(func() {
				envelope.Payload = protoutil.MarshalOrPanic(&cb.Payload{
					Header: nil,
				})
			})

			ginkgo.It("sends a bad request message", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_BAD_REQUEST))
			})

			ginkgo.It("sends a bad envelope to HandleAttestation", func() {
				err := handler.HandleAttestation(context.Background(), server, envelope)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_BAD_REQUEST))
			})
		})

		ginkgo.Context("when unmarshalling the channel header fails", func() {
			ginkgo.BeforeEach(func() {
				channelHeaderPayload = []byte("complete-nonsense")
			})

			ginkgo.It("sends a bad request message", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_BAD_REQUEST))
			})
		})

		ginkgo.Context("when the channel header timestamp is nil", func() {
			ginkgo.BeforeEach(func() {
				channelHeaderPayload = protoutil.MarshalOrPanic(&cb.ChannelHeader{
					Timestamp: nil,
				})
			})

			ginkgo.It("sends a bad request message", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_BAD_REQUEST))
			})
		})

		ginkgo.Context("when the channel header timestamp is out of the time window", func() {
			ginkgo.BeforeEach(func() {
				channelHeaderPayload = protoutil.MarshalOrPanic(&cb.ChannelHeader{
					Timestamp: &timestamppb.Timestamp{},
				})
			})

			ginkgo.It("sends status bad request", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_BAD_REQUEST))
			})
		})

		ginkgo.Context("when the channel is not found", func() {
			ginkgo.BeforeEach(func() {
				fakeChainManager.GetChainReturns(nil)
			})

			ginkgo.It("sends status not found", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_NOT_FOUND))
			})
		})

		ginkgo.Context("when the client disconnects before reading from the chain", func() {
			var (
				ctx    context.Context
				cancel func()
				done   chan struct{}
			)

			ginkgo.BeforeEach(func() {
				done = make(chan struct{})
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
				fakeBlockIterator.NextStub = func() (*cb.Block, cb.Status) {
					<-done
					return nil, cb.Status_BAD_REQUEST
				}
			})

			ginkgo.AfterEach(func() {
				close(done)
			})

			ginkgo.It("aborts the deliver stream", func() {
				err := handler.Handle(ctx, server)
				gomega.Expect(err).To(gomega.MatchError("context finished before block retrieved: context canceled"))
			})
		})

		ginkgo.Context("when the chain errors before reading from the chain", func() {
			ginkgo.BeforeEach(func() {
				close(errCh)
			})

			ginkgo.It("sends status service unavailable", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeChain.ReaderCallCount()).To(gomega.Equal(0))
				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_SERVICE_UNAVAILABLE))
			})

			ginkgo.Context("when the seek info requests a best effort error response", func() {
				ginkgo.BeforeEach(func() {
					seekInfo.ErrorResponse = ab.SeekInfo_BEST_EFFORT
				})

				ginkgo.It("replies with the desired blocks", func() {
					err := handler.Handle(context.Background(), server)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					gomega.Expect(fakeResponseSender.SendBlockResponseCallCount()).To(gomega.Equal(1))
					gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
					resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
					gomega.Expect(resp).To(gomega.Equal(cb.Status_SUCCESS))
				})
			})
		})

		ginkgo.Context("when the chain errors while reading from the chain", func() {
			var doneCh chan struct{}

			ginkgo.BeforeEach(func() {
				doneCh = make(chan struct{})
				fakeBlockIterator.NextStub = func() (*cb.Block, cb.Status) {
					<-doneCh
					return &cb.Block{}, cb.Status_INTERNAL_SERVER_ERROR
				}
				fakeChain.ReaderStub = func() blockledger.Reader {
					close(errCh)
					return fakeBlockReader
				}
			})

			ginkgo.AfterEach(func() {
				close(doneCh)
			})

			ginkgo.It("sends status service unavailable", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeChain.ReaderCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_SERVICE_UNAVAILABLE))
			})
		})

		ginkgo.Context("when the access evaluation fails", func() {
			ginkgo.BeforeEach(func() {
				fakePolicyChecker.CheckPolicyReturns(errors.New("no-access-for-you"))
			})

			ginkgo.It("sends status not found", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_FORBIDDEN))
			})

			ginkgo.It("records requests received, (unsuccessful) requests completed, and (zero) blocks sent", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeRequestsReceived.AddCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeRequestsReceived.AddArgsForCall(0)).To(gomega.BeNumerically("~", 1.0))
				gomega.Expect(fakeRequestsReceived.WithCallCount()).To(gomega.Equal(1))
				labelValues := fakeRequestsReceived.WithArgsForCall(0)
				gomega.Expect(labelValues).To(gomega.Equal([]string{
					"channel", "chain-id",
					"filtered", "false",
					"data_type", "block",
				}))

				gomega.Expect(fakeBlocksSent.AddCallCount()).To(gomega.Equal(0))
				gomega.Expect(fakeBlocksSent.WithCallCount()).To(gomega.Equal(0))

				gomega.Expect(fakeRequestsCompleted.AddCallCount()).To(gomega.Equal(1))
				gomega.Expect(fakeRequestsCompleted.AddArgsForCall(0)).To(gomega.BeNumerically("~", 1.0))
				gomega.Expect(fakeRequestsCompleted.WithCallCount()).To(gomega.Equal(1))
				labelValues = fakeRequestsCompleted.WithArgsForCall(0)
				gomega.Expect(labelValues).To(gomega.Equal([]string{
					"channel", "chain-id",
					"filtered", "false",
					"data_type", "block",
					"success", "false",
				}))
			})
		})

		ginkgo.Context("when the access expires", func() {
			ginkgo.BeforeEach(func() {
				fakeChain.SequenceStub = func() uint64 {
					return uint64(fakeChain.SequenceCallCount())
				}
				fakePolicyChecker.CheckPolicyReturnsOnCall(1, errors.New("no-access-for-you"))
			})

			ginkgo.It("sends status not found", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_FORBIDDEN))

				gomega.Expect(fakePolicyChecker.CheckPolicyCallCount()).To(gomega.Equal(2))
			})
		})

		ginkgo.Context("when unmarshalling seek info fails", func() {
			ginkgo.BeforeEach(func() {
				seekInfoPayload = []byte("complete-nonsense")
			})

			ginkgo.It("sends status bad request", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_BAD_REQUEST))
			})
		})

		ginkgo.Context("when seek start and stop are nil", func() {
			ginkgo.BeforeEach(func() {
				seekInfo = &ab.SeekInfo{Start: nil, Stop: nil}
			})

			ginkgo.It("sends status bad request", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_BAD_REQUEST))
			})
		})

		ginkgo.Context("when seek info start number is greater than stop number", func() {
			ginkgo.BeforeEach(func() {
				seekInfo = &ab.SeekInfo{
					Start: seekNewest,
					Stop: &ab.SeekPosition{
						Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: 99}},
					},
				}
			})

			ginkgo.It("sends status bad request", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_BAD_REQUEST))
			})
		})

		ginkgo.Context("when fail if not ready is set and the next block is unavailable", func() {
			ginkgo.BeforeEach(func() {
				fakeBlockReader.HeightReturns(1000)
				fakeBlockReader.IteratorReturns(fakeBlockIterator, 1000)

				seekInfo = &ab.SeekInfo{
					Behavior: ab.SeekInfo_FAIL_IF_NOT_READY,
					Start: &ab.SeekPosition{
						Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: 1002}},
					},
					Stop: &ab.SeekPosition{
						Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: 1003}},
					},
				}
			})

			ginkgo.It("sends status not found", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeBlockIterator.NextCallCount()).To(gomega.Equal(0))
				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_NOT_FOUND))
			})
		})

		ginkgo.Context("when next block status does not indicate success", func() {
			ginkgo.BeforeEach(func() {
				fakeBlockIterator.NextReturns(nil, cb.Status_UNKNOWN)
			})

			ginkgo.It("forwards the status response", func() {
				err := handler.Handle(context.Background(), server)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				gomega.Expect(fakeResponseSender.SendStatusResponseCallCount()).To(gomega.Equal(1))
				resp := fakeResponseSender.SendStatusResponseArgsForCall(0)
				gomega.Expect(resp).To(gomega.Equal(cb.Status_UNKNOWN))
			})
		})
	})
})
