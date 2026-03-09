// Copyright 2024 The Artemis Authors
// Licensed under the Apache License, Version 2.0

//go:build builtinassets

package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed dist
var embedFS embed.FS

// Assets contains the embedded UI files (when built with -tags builtinassets)
var Assets http.FileSystem

func init() {
	// Strip the "dist" prefix from the embedded filesystem
	distFS, err := fs.Sub(embedFS, "dist")
	if err != nil {
		panic(err)
	}
	Assets = http.FS(distFS)
}
