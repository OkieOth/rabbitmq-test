#!/bin/bash
# stop the test environment

scriptPos=${0%/*}

source $scriptPos/stackConf.sh

docker stack rm $STACK_NAME
