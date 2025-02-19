#!/bin/bash

set -e
set -o pipefail
set -u

wget https://builds.openlogic.com/downloadJDK/openlogic-openjdk/11.0.26+4/openlogic-openjdk-11.0.26+4-linux-x64.tar.gz -O openjdk-11.tar.gz
mkdir velox4j-openjdk-11
tar -xvf openjdk-11.tar.gz --strip-components 1 -C velox4j-openjdk-11
wget https://archive.apache.org/dist/maven/maven-3/3.9.2/binaries/apache-maven-3.9.2-bin.tar.gz -O maven.tar.gz
mkdir velox4j-maven
tar -xvf maven.tar.gz --strip-components 1 -C velox4j-maven
