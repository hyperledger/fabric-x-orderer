/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"bytes"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"strings"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"google.golang.org/protobuf/reflect/protoreflect"
	"gopkg.in/yaml.v3"
)

// logConfiguration logs the local and shared startup configuration.
func logConfiguration(logger *flogging.FabricLogger, conf *Configuration, role string, blockNumber uint64) {
	logYAML(logger, fmt.Sprintf("Local config (%s)", role), localConfigForLog(conf.LocalConfig.NodeLocalConfig))

	logYAML(logger, fmt.Sprintf("Shared config (block %d)", blockNumber), protoTree(conf.SharedConfig.ProtoReflect()))
}

func logYAML(logger *flogging.FabricLogger, title string, value any) {
	data, err := yaml.Marshal(value)
	if err != nil {
		logger.Warnf("%s: failed to render configuration: %v", title, err)
		return
	}
	logger.Infof("%s:\n%s", title, strings.TrimSuffix(string(data), "\n"))
}

// localConfigForLog copies the local config with only safe BCCSP fields.
func localConfigForLog(source *NodeLocalConfig) *NodeLocalConfig {
	node := *source
	general := *source.GeneralConfig
	node.GeneralConfig = &general

	safe := &factory.FactoryOpts{Default: general.BCCSP.Default}
	if sw := general.BCCSP.SW; sw != nil {
		safe.SW = &factory.SwOpts{Hash: sw.Hash, Security: sw.Security}
		if sw.FileKeystore != nil {
			safe.SW.FileKeystore = &factory.FileKeystoreOpts{KeyStorePath: sw.FileKeystore.KeyStorePath}
		}
	}
	general.BCCSP = safe

	return &node
}

// protoTree converts protobuf fields for logging, formatting certificates as Base64.
func protoTree(msg protoreflect.Message) map[string]any {
	out := make(map[string]any)
	fields := msg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.IsMap() {
			continue
		}
		if field.IsList() {
			list := msg.Get(field).List()
			values := make([]any, list.Len())
			for j := 0; j < list.Len(); j++ {
				values[j] = protoValue(field, list.Get(j))
			}
			out[string(field.Name())] = values
			continue
		}
		if field.Message() == nil || msg.Has(field) {
			out[string(field.Name())] = protoValue(field, msg.Get(field))
		}
	}
	return out
}

func protoValue(field protoreflect.FieldDescriptor, value protoreflect.Value) any {
	switch field.Kind() {
	case protoreflect.BytesKind:
		return certificatesForLog(value.Bytes())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return protoTree(value.Message())
	default:
		return value.Interface()
	}
}

// certificatesForLog returns Base64-encoded DER certificates or redacts invalid input.
func certificatesForLog(data []byte) string {
	if len(data) == 0 {
		return "<empty>"
	}

	var certs []string
	var decoded []byte
	for block, rest := pem.Decode(data); block != nil; block, rest = pem.Decode(rest) {
		if _, err := x509.ParseCertificate(block.Bytes); err != nil || block.Type != "CERTIFICATE" || len(block.Headers) > 0 {
			break
		}
		certs = append(certs, base64.StdEncoding.EncodeToString(block.Bytes))
		decoded = append(decoded, pem.EncodeToMemory(block)...)
	}
	if len(certs) == 0 || !bytes.Equal(bytes.Join(bytes.Fields(decoded), nil), bytes.Join(bytes.Fields(data), nil)) {
		return "<redacted, not a valid certificate>"
	}
	return strings.Join(certs, ",")
}
