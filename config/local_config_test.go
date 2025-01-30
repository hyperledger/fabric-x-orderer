package config_test

import (
	"os"
	"path"
	"testing"

	"arma/common/utils"
	"arma/config"
	"arma/testutil"

	"github.com/stretchr/testify/require"
)

func TestLocalConfigLoad(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "load")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	routerLocalConfig := testutil.CreateTestRouterLocalConfig()

	configPath := path.Join(dir, "local_router_config.yaml")
	err = utils.WriteToYAML(routerLocalConfig, configPath)
	require.NoError(t, err)

	routerLocalConfigLoaded, err := config.Load(configPath)
	require.NoError(t, err)
	require.Equal(t, *routerLocalConfigLoaded, *routerLocalConfig)
}
