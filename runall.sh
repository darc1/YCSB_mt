#!/bin/bash

EXPORT_PREFIX=$1
THREAD_COUNT=${2:-8}
POLICY_DIR=${3}

./runall200k.sh $EXPORT_PREFIX $THREAD_COUNT $POLICY_DIR
./runall2m.sh $EXPORT_PREFIX $THREAD_COUNT $POLICY_DIR
./runall20m.sh $EXPORT_PREFIX $THREAD_COUNT $POLICY_DIR
