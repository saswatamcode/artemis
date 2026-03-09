// Copyright 2024 The Artemis Authors
// Licensed under the Apache License, Version 2.0

//go:build !builtinassets

package ui

import (
	"net/http"
	"os"
	"path/filepath"
)

// Assets contains the UI files from the filesystem (when built without -tags builtinassets)
var Assets http.FileSystem

func init() {
	// Determine the UI dist directory path
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	// Try to find the ui/dist directory relative to working directory
	var uiPath string
	switch filepath.Base(wd) {
	case "artemis":
		// When running from repo root
		uiPath = "./ui/dist"
	case "cmd":
		// When running from cmd directory
		uiPath = "../ui/dist"
	default:
		// Default fallback
		uiPath = "./ui/dist"
	}

	// Check if directory exists
	if _, err := os.Stat(uiPath); os.IsNotExist(err) {
		// UI not built, use empty filesystem
		Assets = http.Dir("")
	} else {
		Assets = http.Dir(uiPath)
	}
}
