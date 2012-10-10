#!/bin/bash

#############################################
#You have to put your data initially in the
#location specified by $dataDir of this  
#script.For query, you need to put it in the
#location shown in $queryFile. Then just 
#run the script.
#
#To see output:
#Since results are stored in hdfs in sequence
#format, you need to run the following to 
#read them as:
#hjar target/kddknn.jar readseq <part-0000>
#############################################

dataDir=../data/test
queryFile=../data/query/u1

HADOOP=/Users/Hadoop/hadoop-0.20.2/bin/hadoop
xmlconf=src/main/resources/kddknn/conf.xml
knnjar=target/kddknn.jar

$HADOOP dfs -rmr knn-input
$HADOOP dfs -put $dataDir knn-input
#$HADOOP jar $knnjar invindex -conf $xmlconf 
#$HADOOP jar $knnjar invertedknn -conf $xmlconf 

$HADOOP dfs -rmr query
$HADOOP dfs -put $queryFile query
$HADOOP jar $knnjar queryknn -conf $xmlconf 
