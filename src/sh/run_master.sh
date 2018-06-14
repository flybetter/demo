#!/bin/bash

/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/spark2/spark-submit --class WordCount \
--master yarn \
--deploy-mode client \
--driver-memory 3G \
--executor-memory 6G \
--executor-cores 3 \
--conf "spark.kryoserializer.buffer=32m" \
--conf "spark.kryoserializer.buffer.max=128m" \
--jars "/database/recom_test/lib/jedis-2.9.0.jar" \
./demo-1.0-SNAPSHOT.jar
