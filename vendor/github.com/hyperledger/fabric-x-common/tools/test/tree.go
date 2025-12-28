/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"bytes"
	"os/exec"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

// RequireTree verifies an expected tree.
func RequireTree(t *testing.T, root string, expectedFiles, expectedDirs []string) {
	t.Helper()
	actualTree := GetTree(t, root)

	for _, file := range expectedFiles {
		require.FileExistsf(t, path.Join(root, file), "File '%s' Does not exist. Tree: %s", file, actualTree)
	}
	for _, dir := range expectedDirs {
		require.DirExistsf(t, path.Join(root, dir), "Directory '%s' Does not exist. Tree: %s", dir, actualTree)
	}
}

// GetTree returns a folder's tree for debugging.
func GetTree(t *testing.T, root string) string {
	t.Helper()
	var stdout bytes.Buffer
	cmd := exec.Command("tree")
	cmd.Dir = root
	cmd.Stdout = &stdout
	require.NoError(t, cmd.Run(), "Error running tree")
	return stdout.String()
}
