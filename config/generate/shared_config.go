package generate

// SharedConfig holds the initial configuration that will be used to bootstrap new nodes.
// This configuration is common to all Arma nodes.
type SharedConfig struct {
	PartiesConfig   []PartyConfig   `yaml:"Parties,omitempty"`
	ConsensusConfig ConsensusConfig `yaml:"Consensus,omitempty"`
	BatchingConfig  BatchingConfig  `yaml:"Batching,omitempty"`
}
