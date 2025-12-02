/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

type (
	// OrdererEndpoint defines an orderer party's endpoint.
	OrdererEndpoint struct {
		Host string `mapstructure:"host" json:"host,omitempty" yaml:"host,omitempty"`
		Port int    `mapstructure:"port" json:"port,omitempty" yaml:"port,omitempty"`
		// ID is the party ID.
		ID    uint32 `mapstructure:"id" json:"id,omitempty" yaml:"id,omitempty"`
		MspID string `mapstructure:"msp-id" json:"msp-id,omitempty" yaml:"msp-id,omitempty"`
		// API should be broadcast and/or deliver. Empty value means all API is supported.
		API []string `mapstructure:"api" json:"api,omitempty" yaml:"api,omitempty"`
	}
)

const (
	// Broadcast support by endpoint.
	Broadcast = "broadcast"
	// Deliver support by endpoint.
	Deliver = "deliver"
	// NoID indicates that a party ID is not specified (default).
	// This allows backward compatibility with orderers that doesn't support this syntax.
	NoID = uint32(0x100000)
)

// ErrInvalidEndpoint orderer endpoints error.
var ErrInvalidEndpoint = errors.New("invalid endpoint")

// Address returns a string representation of the endpoint's address.
func (e *OrdererEndpoint) Address() string {
	return net.JoinHostPort(e.Host, strconv.Itoa(e.Port))
}

// String returns a deterministic representation of the endpoint.
func (e *OrdererEndpoint) String() string {
	var output strings.Builder
	isFirst := true
	if e.ID < NoID {
		output.WriteString("id=")
		output.WriteString(strconv.FormatUint(uint64(e.ID), 10))
		isFirst = false
	}
	if len(e.MspID) > 0 {
		if !isFirst {
			output.WriteRune(',')
		}
		output.WriteString("msp-id=")
		output.WriteString(e.MspID)
		isFirst = false
	}
	for _, api := range e.API {
		if !isFirst {
			output.WriteRune(',')
		}
		output.WriteString(api)
		isFirst = false
	}
	if len(e.Host) > 0 || e.Port > 0 {
		if !isFirst {
			output.WriteRune(',')
		}
		output.WriteString(e.Address())
	}
	return output.String()
}

// SupportsAPI returns true if this endpoint supports API.
// It also returns true if no APIs are specified, as we cannot know.
func (e *OrdererEndpoint) SupportsAPI(api string) bool {
	return len(e.API) == 0 || slices.Contains(e.API, api)
}

// ParseOrdererEndpoint parses a string according to the following schema order (the first that succeeds).
// Schema 1: YAML.
// Schema 2: JSON.
// Schema 3: [id=ID,][msp-id=MspID,][broadcast,][deliver,][host=Host,][port=Port,][Host:Port].
func ParseOrdererEndpoint(valueRaw string) (*OrdererEndpoint, error) {
	ret := &OrdererEndpoint{ID: NoID}
	if len(valueRaw) == 0 {
		return ret, nil
	}
	if err := yaml.Unmarshal([]byte(valueRaw), ret); err == nil {
		return ret, nil
	}
	if err := json.Unmarshal([]byte(valueRaw), ret); err == nil {
		return ret, nil
	}
	err := unmarshalOrdererEndpoint(valueRaw, ret)
	return ret, err
}

func unmarshalOrdererEndpoint(valueRaw string, out *OrdererEndpoint) error {
	metaParts := strings.Split(valueRaw, ",")
	for _, item := range metaParts {
		item = strings.TrimSpace(item)
		equalIdx := strings.Index(item, "=")
		colonIdx := strings.Index(item, ":")
		var err error
		switch {
		case item == Broadcast || item == Deliver:
			out.API = append(out.API, item)
		case equalIdx >= 0:
			key, value := strings.TrimSpace(item[:equalIdx]), strings.TrimSpace(item[equalIdx+1:])
			switch key {
			case "msp-id":
				out.MspID = value
			case "host":
				out.Host = value
			case "id":
				err = out.setID(value)
			case "port":
				err = out.setPort(value)
			default:
				return fmt.Errorf("invalid key '%s' for item '%s': %w", key, item, ErrInvalidEndpoint)
			}
		case colonIdx >= 0:
			var port string
			out.Host, port, err = net.SplitHostPort(strings.TrimSpace(item))
			if err != nil {
				return fmt.Errorf("invalid host/port '%s': %w", item, err)
			}
			err = out.setPort(strings.TrimSpace(port))
		default:
			return fmt.Errorf("invalid item '%s': %w", item, ErrInvalidEndpoint)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *OrdererEndpoint) setPort(portStr string) error {
	port, err := strconv.ParseInt(portStr, 10, 32)
	if err != nil {
		return fmt.Errorf("failed to parse port: %w", err)
	}
	e.Port = int(port)
	return nil
}

func (e *OrdererEndpoint) setID(idStr string) error {
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		return fmt.Errorf("invalid id value: %w", err)
	}
	e.ID = uint32(id)
	return nil
}
