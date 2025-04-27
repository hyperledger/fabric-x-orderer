package fabric

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/msp/mgmt"
)

// GetDevMspDir gets the path to the fabric/sampleconfig/msp tree that is maintained with the source tree.
// This should only be used in a test/development context.
func GetDevMspDir() string {
	devDir := GetDevConfigDir()
	return filepath.Join(devDir, "msp")
}

// GetDevConfigDir returns the fabric sampleconfig directory.
// This should only be used in a test/development context.
func GetDevConfigDir() string {
	path, err := gomodDevConfigDir()
	if err != nil {
		panic(err)
	}
	return path
}

func SafeGetDevConfigDir() (string, error) {
	return gomodDevConfigDir()
}

func gomodDevConfigDir() (string, error) {
	buf := bytes.NewBuffer(nil)
	cmd := exec.Command("go", "env", "GOMOD")
	cmd.Stdout = buf

	if err := cmd.Run(); err != nil {
		return "", err
	}

	modFile := strings.TrimSpace(buf.String())
	if modFile == "" {
		return "", errors.New("not a module or not in module mode")
	}

	devPath := filepath.Join(filepath.Dir(modFile), "testutil", "fabric", "sampleconfig")
	if !dirExists(devPath) {
		return "", fmt.Errorf("%s does not exist", devPath)
	}

	return devPath, nil
}

func dirExists(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.IsDir()
}

// LoadMSPSetupForTesting sets up the local MSP and a chain MSP for the default chain.
// This should only be used in a test/development context.
func LoadMSPSetupForTesting() error {
	dir := GetDevMspDir()
	conf, err := msp.GetLocalMspConfig(dir, nil, "SampleOrg")
	if err != nil {
		return err
	}

	err = mgmt.GetLocalMSP(factory.GetDefault()).Setup(conf)
	if err != nil {
		return err
	}

	err = mgmt.GetManagerForChain("testchannelid").Setup([]msp.MSP{mgmt.GetLocalMSP(factory.GetDefault())})
	if err != nil {
		return err
	}

	return nil
}

func SetDevFabricConfigPath(t *testing.T) {
	t.Helper()
	t.Setenv("FABRIC_CFG_PATH", GetDevConfigDir())
}
