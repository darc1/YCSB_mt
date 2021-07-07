#!/bin/bash
mvn clean package install -P ycsb-release -DskipTests
rm -r workdir
tar xvf distribution/target/ycsb-0.17.1-SNAPSHOT.tar.gz
mv ycsb-0.17.1-SNAPSHOT/ workdir/
