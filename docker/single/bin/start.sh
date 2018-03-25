#!/bin/bash
#
# Requirements:
# - bash
# - docker

scriptPos=${0%/*}

#include conf script
source $scriptPos/stackConf.sh

composeFile="single_instance.yml"

pushd "$scriptPos/.."

docker stack deploy -c "${composeFile}" "$STACK_NAME"

popd > /dev/null
