#!/bin/bash
#
# creates the python classes for our .proto
#
#
# resources/pipe.proto: File does not reside within any path specified using --proto_path (or -I).  You must specify a --proto_path which encompasses this file.  Note that the proto_path must be an exact prefix of the .proto file names -- protoc is too dumb to figure out when two paths (e.g. absolute and relative) are equivalent (it's harder than you think).
#
#
#
#
###

#project_base="/Users/gash/workspace/messaging/core-netty/python"

project_base="$( cd .. "(cd "$( dirname "${BASH_SOURCE[0]}" )")" && pwd )"

#rm ${project_base}/src/*

protoc -I=${project_base}/resources --python_out=./src ${project_base}/resources/pipe.proto 
protoc -I=${project_base}/resources --python_out=./src ${project_base}/resources/common.proto
