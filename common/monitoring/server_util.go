/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring

import (
	"net"
	"os/exec"
	"strconv"
	"strings"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/pkg/errors"
)

const protocol = "tcp"

type Endpoint struct {
	Host string
	Port int
}

// Address returns a string representation of the endpoint's address.
func (e *Endpoint) Address() string {
	return net.JoinHostPort(e.Host, strconv.Itoa(e.Port))
}

type ServerConfig struct {
	endpoint             *Endpoint
	preAllocatedListener net.Listener
	logger               types.Logger
}

// Listener instantiate a [net.Listener] and updates the config port with the effective port.
func (s *ServerConfig) Listener() (net.Listener, error) {
	if s.preAllocatedListener != nil {
		return s.preAllocatedListener, nil
	}
	listener, err := net.Listen(protocol, s.endpoint.Address())
	if err != nil {
		return nil, errors.Wrap(err, "failed to listen")
	}

	addr := listener.Addr()
	tcpAddress, ok := addr.(*net.TCPAddr)
	if !ok {
		return nil, errors.New(strings.Join([]string{"failed to cast to TCP address", listener.Close().Error()}, "; "))
	}
	s.endpoint.Port = tcpAddress.Port

	s.logger.Infof("Listening on: %s://%s", protocol, s.endpoint.Address())
	return listener, nil
}

// PreAllocateListener is used to allocate a port and bind to ahead of the server initialization.
// It stores the listener object internally to be reused on subsequent calls to Listener().
func (c *ServerConfig) PreAllocateListener() (net.Listener, error) {
	listener, err := c.Listener()
	if err != nil {
		return nil, err
	}
	c.preAllocatedListener = listener
	return listener, nil
}

func FQDN() (string, error) {
	out, err := exec.Command("hostname", "--fqdn").Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}
