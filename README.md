Impala Table Metadata Refresher
===============================

Used for refreshing the metadata on a table across all of the nodes in a cluster. Metadata refreshing
will occurr concurrently, any node which exceeds a 1 minute refresh will timeout. 

This was designed because I needed a way to manually invoke a full retrieve of all the parititions on a given table for
every node in the cluster.

How To Use
----------

An error will be displayed if your node timesout or fails to refresh (non 0 exit code). If any of your nodes fail
to refresh, the exit code of impala-refresher will be 1. 

Usage:

    Usage of ./impala-refresher:
      -concurrency=0: Max number of refreshes to perform concurrently (0: unlimited)
      -nodes="":    Comma separated list of impala daemons to refresh
      -table="":    Table to refresh
      -timeout=60:  Refresh timeout in seconds

Example:

    $ ./impala-refresher --table mytable --nodes node-01,node-02,node-03,node-04,node-05,node-06
    Refreshing node-01's metadata...
    Refreshing node-02's metadata...
    Refreshing node-03's metadata...
    Refreshing node-04's metadata...
    Refreshing node-05's metadata...
    Refreshing node-06's metadata...
    node-06 refreshed successfully! Took: 25.195226472s
    node-05 refreshed successfully! Took: 26.145260728s
    node-04 refreshed successfully! Took: 26.268907552s
    node-03 refreshed successfully! Took: 26.753072643s
    node-01 refreshed successfully! Took: 27.024496418s
    node-02 refreshed successfully! Took: 27.654374677s

With concurrency set to *3*

    $ ./impala-refresher --table mytable --nodes node-01,node-02,node-03,node-04,node-05,node-06 --concurrency 3
    Refreshing node-01's metadata...
    Refreshing node-02's metadata...
    Refreshing node-03's metadata...
    node-02 refreshed successfully! Took: 19.313201ms
    node-01 refreshed successfully! Took: 19.564682ms
    node-03 refreshed successfully! Took: 20.187352ms
    Refreshing node-04's metadata...
    Refreshing node-05's metadata...
    Refreshing node-06's metadata...
    node-04 refreshed successfully! Took: 22.933899ms
    node-05 refreshed successfully! Took: 40.255635ms
    node-06 refreshed successfully! Took: 45.224793ms



How To Build
------------

The only requirement is [go](http://golang.org/), The build time is basically instant. The binary can be used anywhere,
it doesn't require any special libraries on the host as far as I know. I've tested on systems as old as CentOS 5.6.

Building:

    $ git clone git://github.com/rickysaltzer/impala-refresher.git
    $ cd impala-refresher
    $ go build
    $ ls
    impala-refresher.go impala-refresher
