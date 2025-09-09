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

	"github.com/pkg/errors"
)

const grpcProtocol = "tcp"

type Endpoint struct {
	Host string
	Port int
}

// Address returns a string representation of the endpoint's address.
func (e *Endpoint) Address() string {
	return net.JoinHostPort(e.Host, strconv.Itoa(e.Port))
}

type ServerConfig struct {
	Endpoint             Endpoint
	preAllocatedListener net.Listener
}

// Listener instantiate a [net.Listener] and updates the config port with the effective port.
func (c *ServerConfig) Listener() (net.Listener, error) {
	if c.preAllocatedListener != nil {
		return c.preAllocatedListener, nil
	}
	listener, err := net.Listen(grpcProtocol, c.Endpoint.Address())
	if err != nil {
		return nil, errors.Wrap(err, "failed to listen")
	}

	addr := listener.Addr()
	tcpAddress, ok := addr.(*net.TCPAddr)
	if !ok {
		return nil, errors.New(strings.Join([]string{"failed to cast to TCP address", listener.Close().Error()}, "; "))
	}
	c.Endpoint.Port = tcpAddress.Port

	logger.Infof("Listening on: %s://%s", grpcProtocol, c.Endpoint.Address())
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
