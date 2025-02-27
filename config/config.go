package config

import (
	"github.ibm.com/decentralized-trust-research/arma/config/protos"

	"github.com/pkg/errors"
)

// Configuration holds the complete configuration of a database node.
type Configuration struct {
	LocalConfig  *LocalConfig
	SharedConfig *protos.SharedConfig
}

// ReadConfig reads the configurations from the config file and returns it. The configuration includes both local and shared.
func ReadConfig(configFilePath string) (*Configuration, error) {
	if configFilePath == "" {
		return nil, errors.New("path to the configuration file is empty")
	}

	var err error
	conf := &Configuration{}

	conf.LocalConfig, err = LoadLocalConfig(configFilePath)
	if err != nil {
		return nil, err
	}

	switch conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.Method {
	case "yaml":
		if conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File != "" {
			conf.SharedConfig, err = LoadSharedConfig(conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to read the shared configuration from: '%s'", conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File)
			}
		}
	case "block":
		// TODO: complete when block is ready
		return nil, errors.Errorf("not implemented yet")
	default:
		return nil, errors.Errorf("bootstrap method %s is invalid", conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.Method)
	}

	return conf, nil
}
