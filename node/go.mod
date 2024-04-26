module github.ibm.com/Yacov-Manevich/ARMA/node

go 1.21

require github.com/SmartBFT-Go/consensus/v2 v2.3.0

replace arma => ../

replace arma/request => ../request

require (
	arma v0.0.0-00010101000000-000000000000
	github.com/golang/protobuf v1.5.3
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0
	github.com/hyperledger/fabric v1.4.0-rc1.0.20240314152450-c8a6fcc0ffa3
	github.com/hyperledger/fabric-config v0.2.1
	github.com/hyperledger/fabric-lib-go v1.1.1
	github.com/hyperledger/fabric-protos-go v0.3.3
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.9.0
	github.com/syndtr/goleveldb v1.0.1-0.20210305035536-64b5b1c73954
	go.uber.org/zap v1.26.0
	google.golang.org/grpc v1.61.0
	google.golang.org/protobuf v1.33.0
	gopkg.in/yaml.v3 v3.0.1
)

replace github.com/Shopify/sarama => github.com/IBM/sarama v1.43.1

require (
	github.com/IBM/idemix v0.0.2-0.20231011101252-a4feda90f3f7 // indirect
	github.com/IBM/idemix/bccsp/schemes/aries v0.0.0-20231003085036-c4470b87b2d6 // indirect
	github.com/IBM/idemix/bccsp/schemes/weak-bb v0.0.0-20240125153755-b3fcea5c7863 // indirect
	github.com/IBM/idemix/bccsp/types v0.0.0-20240125153755-b3fcea5c7863 // indirect
	github.com/IBM/mathlib v0.0.3-0.20231011094432-44ee0eb539da // indirect
	github.com/Knetic/govaluate v3.0.1-0.20171022003610-9aa49832a739+incompatible // indirect
	github.com/ale-linux/aries-framework-go/component/kmscrypto v0.0.0-20230817163708-4b3de6d91874 // indirect
	github.com/bits-and-blooms/bitset v1.13.0 // indirect
	github.com/consensys/bavard v0.1.13 // indirect
	github.com/consensys/gnark-crypto v0.12.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hyperledger/fabric-amcl v0.0.0-20230602173724-9e02669dceb2 // indirect
	github.com/kilic/bls12-381 v0.1.0 // indirect
	github.com/miekg/pkcs11 v1.1.1 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mmcloughlin/addchain v0.4.0 // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sykesm/zap-logfmt v0.0.4 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.21.0 // indirect
	golang.org/x/net v0.23.0 // indirect
	golang.org/x/sync v0.6.0 // indirect
	golang.org/x/sys v0.18.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231120223509-83a465c0220f // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	rsc.io/tmplfunc v0.0.3 // indirect
)
