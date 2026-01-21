/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cryptogen

import (
	"bytes"
	"text/template"

	"github.com/cockroachdb/errors"
)

type hostnameData struct {
	Prefix string
	Index  int
	Domain string
}

type specData struct {
	Hostname   string
	Domain     string
	CommonName string
}

func renderOrgSpecForOrgUnitWithTemplate(orgSpec *OrgSpec, orgUnit string) error {
	err := renderNodeTemplate(orgSpec, orgUnit)
	if err != nil {
		return err
	}
	forceNodesOrgUnit(orgSpec, orgUnit)
	return renderOrgSpec(orgSpec)
}

func renderOrgSpec(orgSpec *OrgSpec) error {
	// Touch up all general node-specs to add the domain
	for i := range orgSpec.Specs {
		err := renderNodeSpec(orgSpec.Domain, &orgSpec.Specs[i])
		if err != nil {
			return err
		}
	}

	// Process the CA node-spec in the same manner
	if len(orgSpec.CA.Hostname) == 0 {
		orgSpec.CA.Hostname = DefaultCaHostname
	}
	return renderNodeSpec(orgSpec.Domain, &orgSpec.CA)
}

func forceNodesOrgUnit(orgSpec *OrgSpec, orgUnit string) {
	for i := range orgSpec.Specs {
		orgSpec.Specs[i].OrganizationalUnit = orgUnit
	}
}

func renderNodeTemplate(orgSpec *OrgSpec, orgUnit string) error {
	publicKeyAlg := getPublicKeyAlg(orgSpec.Template.PublicKeyAlgorithm)
	// First process all of our templated nodes
	for i := range orgSpec.Template.Count {
		data := hostnameData{
			Prefix: orgUnit,
			Index:  i + orgSpec.Template.Start,
			Domain: orgSpec.Domain,
		}

		hostname, err := parseTemplateWithDefault(orgSpec.Template.Hostname, defaultHostnameTemplate, data)
		if err != nil {
			return err
		}
		if hostname == "" {
			return errors.Newf("hostname is empty: %+v", data)
		}

		orgSpec.Specs = append(orgSpec.Specs, NodeSpec{
			Hostname:           hostname,
			CommonName:         hostname,
			SANS:               orgSpec.Template.SANS,
			PublicKeyAlgorithm: publicKeyAlg,
			OrganizationalUnit: orgUnit,
		})
	}

	return nil
}

func renderNodeSpec(domain string, spec *NodeSpec) error {
	data := specData{
		Hostname:   spec.Hostname,
		CommonName: spec.CommonName,
		Domain:     domain,
	}

	// Process our CommonName
	cn, err := parseTemplateWithDefault(spec.CommonName, defaultCNTemplate, data)
	if err != nil {
		return err
	}

	spec.CommonName = cn
	data.CommonName = cn

	if spec.PublicKeyAlgorithm == "" {
		spec.PublicKeyAlgorithm = ECDSA
	}

	// Save off our original, unprocessed SANS entries
	origSANS := spec.SANS

	// Set our implicit SANS entries for CN/Hostname
	spec.SANS = []string{cn, spec.Hostname}

	// Finally, process any remaining SANS entries
	for _, _san := range origSANS {
		san, parseErr := parseTemplate(_san, data)
		if parseErr != nil {
			return parseErr
		}
		spec.SANS = append(spec.SANS, san)
	}

	return nil
}

func parseTemplate(input string, data any) (string, error) {
	t, err := template.New("parse").Parse(input)
	if err != nil {
		return "", errors.Wrap(err, "error parsing template")
	}

	output := new(bytes.Buffer)
	err = t.Execute(output, data)
	if err != nil {
		return "", errors.Wrap(err, "error executing template")
	}

	return output.String(), nil
}

func parseTemplateWithDefault(input, defaultInput string, data any) (string, error) {
	// Use the default if the input is an empty string
	if len(input) == 0 {
		input = defaultInput
	}
	return parseTemplate(input, data)
}
