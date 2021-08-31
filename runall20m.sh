#!/bin/bash

MISS_RATIOS=(0.01 0.05 0.1 0.2 0.3 0.4 0.5)
EXPORT_PREFIX=$1
THREAD_COUNT=${2:-8}
PROPS_DIR=${3:-policy1k}
RESULTS_DIR=output/results${THREAD_COUNT}t20m${PROPS_DIR}
PROPS=configs/${PROPS_DIR}/local20m.properties
LOG_FILE=run20m.log

rm $LOG_FILE

mkdir -p $RESULTS_DIR

for RATIO in "${MISS_RATIOS[@]}"
do
	EXPORT_FILE="$RESULTS_DIR/$EXPORT_PREFIX$RATIO.json"
	echo "runnig for ratio: $RATIO export file: $EXPORT_FILE"	
	./workdir/bin/ycsb.sh run jdbcmt -P $PROPS -p exportfile=$EXPORT_FILE -p miss_ratio=$RATIO -p threadcount=$THREAD_COUNT &>> $LOG_FILE
done
