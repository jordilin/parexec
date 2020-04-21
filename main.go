// Copyright 2020 Jordi Carrillo. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// parexec executes shell commands sequentially and in parallel given a yaml
// resource description.

package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"runtime"
	"sync"

	"gopkg.in/yaml.v2"
)

type cli struct {
	command string
	args    []string
}

type execfunc func() error

func readYaml(path string) ([]byte, error) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return content, err
	}
	return content, nil
}

type execdataMeta struct {
	Funcs []functionMeta `yaml:"execdata"`
}

type functionMeta struct {
	Name string   `yaml:"name"`
	Cmd  string   `yaml:"cmd"`
	Args []string `yaml:"args"`
}

type functionsMeta struct {
	Ex []execdataMeta `yaml:"functions"`
}

// execData encapsulates functions that need to be executed. It can contain an
// array of functions that execute one after another, i.e second function
// depends on the outcome of the first to be able to execute.
type execData struct {
	fs []execfunc
}

func newexecData() *execData {
	return &execData{}
}

func (e *execData) add(fs execfunc) {
	e.fs = append(e.fs, fs)
}

// executor is a worker that receives data to be executed. The data contains the
// functions to be executed.
// This will run inside a goroutine receiving executable data execData which
// contains an array of functions to be executed one after another.
func executor(edataCh <-chan *execData, wg *sync.WaitGroup) {
	for edata := range edataCh {
		for _, f := range edata.fs {
			err := f()
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	wg.Done()
}

// processConfig reads the config yaml of the functions that need to be
// executed. A top level functions key has an array of execdata (executable
// data), which in turn is an array of functions that will be executed one
// after the other. execdata blocks will be executed in parallel.
// Ex:
//
// ---
// functions:
//   - execdata:
//     - name: "k8s get namespace"
//       cmd: kubectl
//       args: ["get", "ns"]
//   - execdata:
//     - name: echoing
//       cmd: echo
//       args: ["hi there"]
//     - name: lsing
//       cmd: ls
//       args: ["."]
// ---
//
// Will be executed as follows
//
//	         ---> worker-0 => execute [kubectl get ns]
//   master /
//          \
//           ---> worker-1 => execute [echo "hi there", ls "."]
func processConfig(config string) []*execData {
	c, err := readYaml("config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	f := functionsMeta{}
	err = yaml.Unmarshal(c, &f)
	if err != nil {
		log.Fatalf("Error decoding yaml file %v", err)
	}
	var dataExec []*execData
	for _, r := range f.Ex {
		eData := newexecData()
		for _, f := range r.Funcs {
			clargs := &cli{f.Cmd, f.Args}
			fc := buildFunc(clargs)
			eData.add(fc)
		}
		dataExec = append(dataExec, eData)
	}
	return dataExec
}

// buildFunc builds a new execfunc based on configuration parameters.
func buildFunc(clargs *cli) execfunc {
	f := func() error {
		fmt.Printf("executing %v\n", clargs.command)
		cmd := exec.Command(clargs.command, clargs.args...)
		var out bytes.Buffer
		cmd.Stdout = &out
		err := cmd.Run()
		if err != nil {
			return err
		}
		fmt.Println(out.String())
		return nil
	}
	return f
}

func main() {
	eds := processConfig("config.yaml")
	var wg sync.WaitGroup
	workers := runtime.NumCPU()
	wg.Add(workers)
	edCh := make(chan *execData)
	// spawn n workers in charge of execute execData
	for i := 0; i < workers; i++ {
		go executor(edCh, &wg)
	}
	for _, ed := range eds {
		edCh <- ed
	}
	close(edCh)
	wg.Wait()
}
