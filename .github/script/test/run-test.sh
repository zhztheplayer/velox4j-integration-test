#!/bin/bash

set -e
set -o pipefail
set -u

export JAVA_HOME="$(readlink -f velox4j-openjdk-11)"
velox4j-maven/bin/mvn -U clean test
