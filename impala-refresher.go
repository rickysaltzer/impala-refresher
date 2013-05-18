/*
	(c) Copyright 2013 Cloudera, Inc.

	Impala Table Metadata Refresher
	
	Used for refreshing the metadata on a table
	across all of the nodes in a cluster. Metadata
	refreshing will occurr concurrently, any node which
	exceeds a 1 minute refresh will timeout. 
*/
package main 

import (
	"fmt"
	"os"
	"time"
	"os/exec"
	"strings"
	"errors"
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
func ExecuteRefresh(node *ImpalaNode, tableName string, finishRefresh chan<- *ImpalaNode) {
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
		case err := <-refreshFinished:					// Blocking
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

		case <-time.After(time.Second * 60):			// Will return after 60 seconds
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
func RefreshNodes(nodes []*ImpalaNode, tableName string) bool {
	finishRefresh := make(chan *ImpalaNode)
	allNodesRefreshed := true
	for _, node := range nodes {
		fmt.Println("Refreshing " + node.hostName + "'s metadata...")
		go ExecuteRefresh(node, tableName, finishRefresh)
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

func printHelp() {
	fmt.Println("Impala Table Metadata Refresher\n")
	fmt.Println("Usage:")
	fmt.Println("\timpala-refresher <table_name> <list of nodes>")
	fmt.Println("Example:")
	fmt.Println("\timpala-refresher mytable node-01 node-02 node-03 ..")
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
	// Help Checks
	if (!(len(os.Args) > 2)) {
		printHelp()
		os.Exit(1)
	}

	if (os.Args[1] == "help" || os.Args[1] == "--help") {
		printHelp()
		os.Exit(1)
	}

	// Check if we have the Impala Shell
	if (!HasImpalaShell()) {
		fmt.Println("Impala shell (impala-shell) is required!")
		os.Exit(1)
	}


	var nodes []*ImpalaNode
	tableName := os.Args[1]

	for _, nodeArg := range os.Args[2:] {
		nodes = append(nodes, &ImpalaNode{nodeArg, false, nil, time.Duration(0)})
	}
	allNodesRefreshed := RefreshNodes(nodes, tableName)
	if (allNodesRefreshed) {
		os.Exit(0)
	} else {
		os.Exit(1)
	}
}
