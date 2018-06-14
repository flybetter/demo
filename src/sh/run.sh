#!/bin/bash

/database/spark/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --class WordCount \
--master local \
--driver-memory 3G \
--executor-memory 3G \
--executor-cores 3 \
--conf "spark.kryo.registrator=rl.nju.rs365.driver.KryoClassRegistrator" \
--conf "spark.kryoserializer.buffer=32m" \
--conf "spark.kryoserializer.buffer.max=128m" \
--jars "/database/spark_project/jedis-2.9.0.jar" \
./demo-1.0-SNAPSHOT.jar
