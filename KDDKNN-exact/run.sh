#!/usr/bin/env bash

#############################################
# Usage: run.sh [clean]
#
# Run by itself, the script will populate the
# HDF with a dataset and process it. Then it
# will ask a song to query and retunr the
# result.
#
# clean: remove all files from the HDF made
# with this script.
#
# You have to put your data initially in the
# location specified by $dataDir of this
# script.For query, you need to put it in the
# location shown in $queryFile. Then just
# run the script.
#
# To see output:
# Since results are stored in hdfs in sequence
# format, you need to run the following to
# read them as:
# hjar target/kddknn.jar readseq <part-0000>
#############################################

# Commands:
CLEAN="clean"
LOAD="load"
INVERT="invert"
KNN="knn"
QUERY="query"


dataDir=/home/yahooData/ydata-ymusic-kddcup-2011-track1/trainIdx1.txt
queryFile=../data/query/u1
#JAVA_HOME=/usr/lib/jvm/jdk1.7.0
HADOOP=$HADOOP/bin/hadoop
xmlconf=src/main/resources/kddknn/conf.xml
knnjar=target/kddknn.jar


## Start run script ##
clean() {
	$HADOOP dfs -rmr sinvertedindex
	$HADOOP dfs -rmr knn-output
#	$HADOOP dfs -rmr knn-input
	$HADOOP dfs -rmr query
}

load() {
  $HADOOP dfs -put $dataDir knn-input
}

invert() {
  $HADOOP jar $knnjar invindex -conf $xmlconf
}

knn() {
  $HADOOP jar $knnjar invertedknn -conf $xmlconf
}

if [[ $1 = $CLEAN ]]; then
	clean
	ant clean
	exit
fi

if [[ $1 = $LOAD ]]; then
	load
	exit
fi

# Build from source.
ant build

if [[ $1 = $INVERT ]]; then
	invert
	exit
fi

if [[ $1 = $KNN ]]; then
    knn
	exit
fi

#$HADOOP dfs -put $queryFile query
#$HADOOP jar $knnjar queryknn -conf $xmlconf
