#!/bin/bash
sudo sudo docker exec -it spark_spark_1 /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 /home/workspace/lesson-2-joins-and-json/exercises/starter/bank-deposits.py | tee ../../../spark/logs/bank-deposits.log