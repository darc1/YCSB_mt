#!/bin/bash

EXPORT_PREFIX=$1
THREAD_COUNT=${2:-8}
POLICY_DIR=${3}

echo "running with policy 50"
./runall20m.sh "${EXPORT_PREFIX}1r" $THREAD_COUNT policy50
./runall20m.sh "${EXPORT_PREFIX}2r" $THREAD_COUNT policy50
./runall20m.sh "${EXPORT_PREFIX}3r" $THREAD_COUNT policy50

echo "running with policy 1k"
./runall20m.sh "${EXPORT_PREFIX}1r" $THREAD_COUNT policy1k
./runall20m.sh "${EXPORT_PREFIX}2r" $THREAD_COUNT policy1k
./runall20m.sh "${EXPORT_PREFIX}3r" $THREAD_COUNT policy1k

echo "running with policy 10k"
./runall20m.sh "${EXPORT_PREFIX}1r" $THREAD_COUNT policy10k
./runall20m.sh "${EXPORT_PREFIX}2r" $THREAD_COUNT policy10k
./runall20m.sh "${EXPORT_PREFIX}3r" $THREAD_COUNT policy10k
