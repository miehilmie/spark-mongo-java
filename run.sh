#!/bin/sh

PATH_TO_SPARK=/home/miehilmie/spark-1.5.1-bin-hadoop2.6;
$PATH_TO_SPARK/bin/spark-submit --master local[4] \
  --jars build/output/lib/mongo-hadoop-core-1.4.1.jar,build/output/lib/mongo-java-driver-3.0.4.jar \
  build/libs/spark-mongo.jar

