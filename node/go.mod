module github.ibm.com/Yacov-Manevich/ARMA/node

go 1.20

require github.com/SmartBFT-Go/consensus/v2 v2.3.0

replace github.com/SmartBFT-Go/consensus v0.0.0-20231206083457-f8b15a205b36 => github.com/fabric-research/consensus v0.0.0-20240117130510-a126c0e8ac53

require (
	github.com/golang/protobuf v1.5.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
)
