package assembler

import (
	"arma/common/types"
	"arma/node/config"
)

func partiesFromAssemblerConfig(config config.AssemblerNodeConfig) []types.PartyID {
	var parties []types.PartyID
	for _, b := range config.Shards[0].Batchers {
		parties = append(parties, types.PartyID(b.PartyID))
	}
	return parties
}
