package assembler

import (
	"arma/core"
	"arma/node/config"
)

func partiesFromAssemblerConfig(config config.AssemblerNodeConfig) []core.PartyID {
	var parties []core.PartyID
	for _, b := range config.Shards[0].Batchers {
		parties = append(parties, core.PartyID(b.PartyID))
	}
	return parties
}
