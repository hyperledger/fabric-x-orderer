package assembler

import (
	arma "arma/core"
	"arma/node/config"
)

func partiesFromAssemblerConfig(config config.AssemblerNodeConfig) []arma.PartyID {
	var parties []arma.PartyID
	for _, b := range config.Shards[0].Batchers {
		parties = append(parties, arma.PartyID(b.PartyID))
	}
	return parties
}
