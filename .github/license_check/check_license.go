// Copyright (c) ByteDance Ltd. and/or its affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/bmatcuk/doublestar/v4"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Header struct {
		PathsIgnore []string `yaml:"paths-ignore"`
	} `yaml:"header"`
}

func main() {
	files := os.Args[1:]
	if len(files) == 0 {
		fmt.Printf("[INFO] No files to check.\n")
		return
	}
	cwd, err := os.Getwd()

	if err != nil {
		panic(err)
	}

	projectRoot, err := filepath.Abs(filepath.Join(cwd, "../../"))
	if err != nil {
		panic(err)
	}
	configFile := filepath.Join(projectRoot, ".licenserc.yaml")

	fmt.Printf("[INFO] Project Root: %s\n", projectRoot)
	fmt.Printf("[INFO] Config Path:  %s\n", configFile)

	// 1. print files to check
	fmt.Printf("[INFO] Files to check: %v\n", files)

	// 2. read and parse config file
	ignores, err := loadIgnorePatterns(configFile)
	if err != nil {
		fmt.Printf("[WARNING] Failed to load config: %v\n", err)
	}

	// 3. filter files
	validFiles := filterFiles(files, ignores)
	if len(validFiles) == 0 {
		fmt.Printf("[INFO] All files are ignored.\n")
		os.Exit(0)
	}
	fmt.Printf("[INFO] Valid files to check: %v\n", validFiles)
	// 4.1 print license-eye command
	fmt.Printf("[INFO] license-eye command: license-eye header check %v -c %s\n", validFiles, configFile)

	args := append([]string{"header", "check"}, validFiles...)
	args = append(args, "-c", configFile)

	cmd := exec.Command("license-eye", args...)
	cmd.Dir = projectRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			os.Exit(exitError.ExitCode())
		}
		fmt.Printf("[ERROR] Failed to run license-eye: %v\n", err)
		os.Exit(1)
	}
}

func loadIgnorePatterns(configFile string) ([]string, error) {
	data, err := os.ReadFile(configFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return cfg.Header.PathsIgnore, nil
}

func filterFiles(files []string, patterns []string) []string {
	if len(patterns) == 0 {
		return files
	}

	var valid []string
	for _, file := range files {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			continue
		}
		ignored := false
		for _, pattern := range patterns {
			// use doublestar to support **/ pattern
			matched, _ := doublestar.Match(pattern, file)
			if matched {
				ignored = true
				break
			}
		}
		if !ignored {
			valid = append(valid, file)
		}
	}
	return valid
}
