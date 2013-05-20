/*
	Copyright 2013 Cloudera, inc

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package main 

import (
	"fmt"
	"os"
	"time"
	"os/exec"
	"strings"
	"errors"
	"flag"
)

type ImpalaNode struct {
	// Host name of Impala daemon
	hostName string

	// Has this node been refreshed?
	refreshed bool

	// Any error that may have occurred during refresh
	refreshError error

	// Total time taken to refresh
	totalRefreshTime time.Duration
}

/*
	Uses the exec library to open impala-shell in order
	to manually refresh an Impala table.
*/
func ExecuteRefresh(node *ImpalaNode, tableName string, timeout int, finishRefresh chan<- *ImpalaNode) {
	// Construct the refresh command using the impala-shell
	refreshCommand := exec.Command("impala-shell", "-i", node.hostName,
		"-q", "refresh " + tableName + "; DESCRIBE " + tableName)

	// Execute a goroutine to block waiting for the command output
	startTime := time.Now()
	refreshFinished := make(chan error)
	go func() {
		output, err := refreshCommand.Output()

		// Confirm that the table was successfully refreshed
		if (!strings.Contains(string(output), "Successfully refreshed table") && err == nil) {
			err = errors.New(node.hostName + "'s catalog did not refresh")
		}

		refreshFinished <- err
	}()

	// Wait for the refresh to finish, timeout after 1 minute.
	select {
		// Waits for refresh command to finish
		case err := <-refreshFinished:
			node.totalRefreshTime = (time.Now().Sub(startTime))
			// Check for any errors (exit 1+)
			if (err != nil) {
				fmt.Println(err)
				node.refreshed = false
				node.refreshError = err
			} else {
				node.refreshed = true
			}
			finishRefresh <- node

		// Timeout channel
		case <-time.After(time.Second * time.Duration(timeout)):
			// Kill the process
			refreshCommand.Process.Kill()
			fmt.Println("Node " + node.hostName + " timed out!")
			node.refreshed = false
			finishRefresh <- node
	}
}

/*
	Refresh all of the supplied Impala daemon's metadata
	concurrently. If all nodes refreshed, return true
*/
func RefreshNodes(nodes []*ImpalaNode, tableName string, timeout int) bool {
	finishRefresh := make(chan *ImpalaNode)
	allNodesRefreshed := true
	for _, node := range nodes {
		fmt.Println("Refreshing " + node.hostName + "'s metadata...")
		go ExecuteRefresh(node, tableName, timeout, finishRefresh)
	}

	for i := 0; i < len(nodes); i++ {
		node := <-finishRefresh
		if (!node.refreshed) {
			fmt.Println(node.hostName + " failed to refresh!")
			allNodesRefreshed = false
		} else {
			fmt.Println(node.hostName + " refreshed successfully! Took: " +
				node.totalRefreshTime.String())
		}
	}

  return allNodesRefreshed
}

/*
	Checks if this system has the Impala Shell
*/
func HasImpalaShell() bool {
	_, err := exec.LookPath("impala-shell")
	return err == nil
}

/*
	Main Function
*/ 
func main() {
	// Argument parsing
	timeout := flag.Int("timeout", 60, "Refresh timeout in seconds")
	tableName := flag.String("table", "","Table to refresh")
	nodeList := flag.String("nodes", "","Comma separated list of impala daemons to refresh")
	flag.Parse()

	// Check to make sure required arguments were supplied
	if (*tableName == "" || *nodeList == "") {
		flag.Usage()
		os.Exit(1)
	}

	// Check if we have the Impala Shell
	if (!HasImpalaShell()) {
		fmt.Println("Impala shell (impala-shell) is required!")
		os.Exit(1)
	}

	var nodes []*ImpalaNode

	for _, nodeArg := range strings.Split(*nodeList, ",") {
		nodes = append(nodes, &ImpalaNode{strings.Trim(nodeArg, " "), false, nil, time.Duration(0)})
	}
	allNodesRefreshed := RefreshNodes(nodes, *tableName, *timeout)

	// If all nodes refreshed successfully, exit ok
	if (allNodesRefreshed) {
		os.Exit(0)
	} else {
		os.Exit(1)
	}
}
