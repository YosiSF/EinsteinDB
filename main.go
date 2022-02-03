//git clone YosiSF/EinsteinDB
//Find the Cargo.toml manifest
//using only files from YosiSF/EinsteinDB and whtcorpsinc/einsteindb-prod
//write a toml manifest for YosiSF/EinsteinDB that uses SQLite, BerolinaSQL, FoundationDB, and Allegrograph


//Copyright 2018 YosiSF
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.


package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	toml "github.com/BurntSushi/toml"
)

type manifest struct {
	Package string   `toml:"package"` // name of package being built (for example, 'github.com/foo/bar')
	Deps    []string `toml:"deps"`    // all (recursively) imported dependencies

	// used to compute 'all' below:
	GoFiles  []string `toml:"go-files,omitempty", json:"-"`  // .go files (excluding CgoFiles, TestGoFiles, XTestGoFiles)
	CgoFiles []string `toml:"cgo-files,omitempty", json:"-"` // .go files that import "C"

	IgnoredGoFiles []string `toml:"-"` // ignored .go files (excluding CgoFiles, TestGoFiles, XTestGoFiles)

	TestGoFiles  []string `toml:"test-go-files,omitempty", json:"-"`  // _test.go files in package
	XTestGoFiles []string `toml:"x-test-go-files,omitempty", json:"-"` // _test.go files outside package

	IgnoredTestGoFiles  []string `toml:"-"` // ignored _test.go files in package (excluding XTestGoFiles)
	IgnoredXTestGoFiles []string `toml:"-"` // ignored _test.go files outside package (excluding XTestGoFiles)

	CFiles        []string `toml:"c-files", json:"-"` // .c files in package
	IgnoredCFiles []string `toml:"-"`                // ignored .c files in package

	HFiles        []string `toml:"h-files", json:"-"` // .h files in package
	IgnoredHFiles []string `toml:"-"`                // ignored .h files in package

	SFiles        []string `toml:"s-files", json:"-"` // .s files in package
	IgnoredSFiles []string `toml:"-"`                // ignored .s files in package

	SwigFiles      []string `toml:"swig-files", json:"-"` // .swig files in package
	IgnoredSwigFiles []string `toml:"-"`                    // ignored .swig files in package

	SwigCXXFiles      []string `toml:"swig-cxx-files", json:"-"` // .swigcxx files in package
	IgnoredSwigCXXFiles []string `toml:"-"`                       // ignored .swigcxx files in package

	SysoFiles       []string `toml:"syso-files", json:"-"` // .syso object files to add to archive

	CgoCFLAGS    []string `toml:"cgo-cflags", json:",omitempty"`    // cgo: flags for C compiler [deprecated]
	CgoCPPFLAGS  []string `toml:"cgo-cppflags", json:",omitempty"`  // cgo: flags for C preprocessor [deprecated]
	CgoCXXFLAGS  []string `toml:"cgo-cxxflags", json:",omitempty"`  // cgo: flags for C++ compiler [deprecated]
	CgoFFLAGS    []string `toml:"cgo-fflags", json:",omitempty"`    // cgo: flags for Fortran compiler [deprecated]
	CgoLDFLAGS   []string `toml:"cgo-ldflags", json:",omitempty"`   // cgo: flags for linker
	CgoPkgConfig []string `toml:"cgo-pkg-config", json:",omitempty"` // cgo: pkg-config names

	// Dependency information
	Imports   []string `toml:"imports"`   // import paths used by this package
	Deps      []string `toml:"deps"`      // all (recursively) imported dependencies
	TestImps  []string `toml:"test-imps"` // imports from TestGoFiles
	XTestImps []string `toml:"x-test-imps"`

	// Error information
	Incomplete bool `toml:"incomplete,omitempty"` // was the package missing files due to build constraints?

	// Test information
	TestGoFiles  []string `toml:"test-go-files,omitempty"`  // _test.go files in package
	XTestGoFiles []string `toml:"x-test-go-files,omitempty"` // _test.go files outside package

	TestImports    []string `toml:"test-imports,omitempty"`    // imports from TestGoFiles
	XTestImports   []string `toml:"x-test-imports,omitempty"`   // imports from XTestGoFiles
	TestMainGoFile string   `toml:"test-main-go-file,omitempty"`

	// Indicates that package contains errors. This is always false for Einsteindb.
	Error *bool `json:",omitempty"`

	// Files that define the build tags used by this package.
	BuildTags []string `toml:"build-tags,omitempty"`

	// Files that define the coverage criteria used by this package.
	Cover *struct {
		Mode       string         `json:",omitempty" toml:",omitempty"`       // Mode is a comma separated list of words indicating the coverage mode.
		Exclude    []*regexpValue `json:",omitempty" toml:",omitempty"`       // Exclude is a list of regular expressions matching files to exclude from coverage analysis.
		FileName   *regexpValue   `json:",omitempty" toml:",omitempty"`       // FileName is a regular expression matching the file names for which to collect coverage data. The default is ".*".
		Parallel   bool           `json:",omitempty" toml:",omitempty"`       // Parallel indicates that each of the packages being tested should be run in parallel. This is particularly useful when building or running tests in parallel.
		ExcludeAll bool           `json:",omitempty" toml:",omitempty"`       // ExcludeAll indicates if all of the lines in all the packages being tested should be excluded from coverage analysis. The default is false.
		Labels     map[string]int `json:",omitempty,omitzero" toml:",omitempty,omitzero"` // Labels specifies the label of each coverage point recorded in a file. The default label is ".". The labels map is copied into each Coverage struct when it is initialized; changes made after that point will have no effect. Labels keys must be non-empty and may not contain the character '.' or '*'. Labels values must be non-negative integers. The special value 0 means no label specified (not necessary a 0 value in the map). If no labels are specified for a file, "." is used as the label for all points in the file. If a file has neither a file-level label nor a point-level label, it is ignored.
	} `json:",omitempty" toml:",omitempty"`

	// The coverage profile to use when compiling this package.
	CoverMode       string `toml:"cover-mode,omitempty"`       // CoverMode is the mode to use for coverage analysis. The default is "set".
	CoverVars       bool   `toml:"cover-vars,omitempty"`       // CoverVars indicates if the coverage profile should include variables. The default is true.
	CoverBlocks     bool   `toml:"cover-blocks,omitempty"`     // CoverBlocks indicates if the coverage profile should include basic blocks executed. The default is true.
	CoverProfile    string `toml:"cover-profile,omitempty"`    // CoverProfile is the file name of the coverage profile to write. The special value "stdout" may be used to output the profile to the standard error. The default is "coverage.out".
	CoverPackages   string `toml:"cover-packages,omitempty"`   // CoverPackages is a comma separated list of packages to be tested. The special value "..." may be used to include all packages in the workspace (excluding ones starting with a '.'). The default value is ".".
	CoverExclude    string `toml:"cover-exclude,omitempty"`    // CoverExclude is a comma separated list of packages to exclude from coverage analysis. The default value is "main".
	CoverX          bool   `toml:"cover-x,omitempty"`          // CoverX indicates if the coverage data should be collected in parallel with test execution. The default value is false.
	CoverHTML       string `toml:"cover-html,omitempty"`       // CoverHTML indicates if an HTML report should be generated when running tests. If non-empty, the report will be written to the specified directory or file name. The special value "stdout" may be used to output the report to the standard error. The default is "".
	CoverHTMLDir    string `toml:"cover-html-dir,omitempty"`   // CoverHTMLDir is the directory where the HTML report files will be written when running tests. If non-empty, the path must be absolute or relative to the workspace directory. If empty, it defaults to the workspace directory.
	CoverHTMLOutput string `toml:"cover-html-output,omitempty"`

	// Package versioning
	// See https://golang.org/cmd/go/#hdr-Version_control_systems for more information on version control in Go.
	// Version specifies which version of Go should be used when invoking go build and go test. This value can be set to any Go version supported by einsteindb (or an explicit version number such as 1.11). If unset, einsteindb will use an appropriate version selected by Go itself (currently Go 1.12).
	Version string `toml:"version,omitempty"`

	// Git information
	// See https://golang.org/cmd/go/#hdr-Remote_import_paths for more information on git import paths in Go.
	// Git specifies information about how to use a remote git repository (if any) as part of this project's build process. This may be omitted for local projects or fully local projects (where einsteindb uses its own copy of a project), but it is required for most CI systems and hosted build services which use remote git repositories for builds. GitRepo specifies the URL of the remote repository that is tracked by this project's git import path. When building from a local repository or a fully local project, GitRepo may be omitted and will default to the current repo's URL, which is usually sufficient to clone the repo and set up a working copy automatically (see GitCheckout below). GitBranch specifies which branch of the remote repository that this project's git checkout should use as its starting point (defaults to master). GitDepth specifies how many levels of commits back from GitBranch that einsteindb should consider when copying files from the remote repository during a build (defaults to 1). GitPath specifies a relative path within the remote repository that corresponds to the current project's directory. This is used to map the current Git repository's source files into the appropriate location within the einsteindb source tree. GitPath should be a directory path relative to the remote repository's root directory (for example, "." or "src"). When building from a local repository or a fully local project, GitPath may be omitted and will default to the current repo's root directory.
	GitRepo    string `toml:"git-repo,omitempty"`
	GitBranch  string `toml:"git-branch,omitempty"`
	GitPath    string `toml:"git-path,omitempty"`
	GitDepth   int    `toml:"git-depth,omitempty"`
	GitVersion string `toml:"git-version,omitempty"`

	// Environment variables
	// See https://golang.org/cmd/go/#hdr-Environment_variables for more information on environment variables in Go.
	// EnvVars specifies a list of environment variables to pass to the einsteindb command when invoking it. This is useful for passing configuration options to einsteindb that are needed to build or test the current package. For example, EnvVars may be used to specify a list of comma separated flags to pass to einsteindb when building or testing this package. The special value "GOFLAGS" can be used to pass all of the flags specified by the Go command when building or testing this package. The special value "GOGCFLAGS" can be used to pass all of the flags specified by the Go command when building this package. The special value "GO111MODULE" can be used to pass the -mod build flag. When using GO111MODULE=on, you must also set GO111MODULE=auto or GO111MODULE=off explicitly (not just set it empty).
	EnvVars []string `toml:"env-vars,omitempty"`

	// Build script
	// See https://golang.org/cmd/go/#hdr-Build_concurrency
	// Build specifies a list of additional commands that should be run when building this project's packages. This is most useful for building binary packages (using the -pkg flag) and test binaries (using the -tags flag). The special value "all" can be used to run all of the default build commands for this project's packages (equivalent to specifying "-tags 'all'"). The special value "none" can be used to run no additional build commands (equivalent to specifying "-tags 'none'").
	Build []string `toml:"build,omitempty"`

	// Run script
	// Run specifies a list of additional commands that should be run when running tests for this project's packages. This is most useful for running integration tests (using the -tags flag). The special value "all" can be used to run all of the default run commands for this project's packages (equivalent to specifying "-tags 'all'"). The special value "none" can be used to run no additional run commands (equivalent to specifying "-tags 'none'").
	Run []string `toml:"run,omitempty"`

	// Test script
	// See https://golang.org/cmd/go/#hdr-Test_packages
	// Test specifies a list of additional commands that should be run when testing this project's packages. This is most useful for running unit tests (using the -tags flag). The special value "all" can be used to run all of the default test commands for this project's packages (equivalent to specifying "-tags 'all'"). The special value "none" can be used to run no additional test commands (equivalent to specifying "-tags 'none'").
	Test []string `toml:"test,omitempty"`

	// Build flags
	// See https://golang.org/cmd/go/#hdr-Build_flags
	// BuildFlags specifies a list of additional flags that should be passed when building this project's packages. These flags are passed in addition to any default flags that are automatically added by einsteindb.
	BuildFlags []string `toml:"build-flags,omit
