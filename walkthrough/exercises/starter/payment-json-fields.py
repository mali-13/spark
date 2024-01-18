from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    BooleanType,
    ArrayType,
    DateType,
)

# Create a spark session, with an appropriately named application name
sparkSession = SparkSession.builder.appName("payment").getOrCreate()
sparkSession.sparkContext.setLogLevel("WARN")

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue", StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr", BooleanType()),
        StructField(
            "zSetEntries",
            ArrayType(
                StructType(
                    [
                        StructField("element", StringType()),
                        StructField("score", StringType()),
                    ]
                )
            ),
        ),
    ]
)

# Read the redis-server kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092,
# configuring the stream to read the earliest messages possible
redisRawStreamingDF = (
    sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "redis-server")
    .option("startingOffsets", "earliest")
    .load()
)

# Using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings,
# and then select them
redisStreamingDF = redisRawStreamingDF.selectExpr(
    "cast(key as string) key", "cast(value as string) value"
)

# Using the redisMessageSchema StructType, deserialize the JSON from the streaming dataframe

# Create a temporary streaming view called "RedisData" based on the streaming dataframe
# it can later be queried with spark.sql
redisStreamingDF.withColumn("value", from_json("value", redisMessageSchema)).select(
    col("value.*")
).createOrReplaceTempView("RedisData")

# Using spark.sql, select key, zSetEntries[0].element as payment from RedisData
reservationPaymentEncodedDF = sparkSession.sql(
    "select key, zSetEntries[0].element as payment from RedisData"
)

# From the dataframe use the unbase64 function to select a column called payment with the base64 decoded JSON,
# and cast it to a string
reservationPaymentDecodedDF = reservationPaymentEncodedDF.withColumn(
    "payment", unbase64("payment").cast("string")
)

# Create a StructType for the Payment schema for the following fields:
# {"reservationId":"9856743232","customerName":"Frank Aristotle","date":"Sep 29, 2020, 10:06:23 AM","amount":"946.88"}
reservationPaymentSchema = StructType(
    [
        StructField("reservationId", StringType()),
        StructField("customerName", StringType()),
        StructField("date", DateType()),
        StructField("amount", StringType()),
    ]
)

# Using the payment StructType, deserialize the JSON from the streaming dataframe, selecting column customer.* as a
# temporary view called Customer
reservationPaymentDecodedDF.withColumn(
    "payment", from_json("payment", reservationPaymentSchema)
).select(col("payment.*")).createOrReplaceTempView("ReservationPayment")

# Using spark.sql select reservationId, amount,  from ReservationPayment
reservationPaymentDF = sparkSession.sql(
    "select reservationId, amount from ReservationPayment"
)

# Write the stream in JSON format to a kafka topic called payment-json, and configure it to run indefinitely,
# the console output will not show any output. You will need to type /data/kafka/bin/kafka-console-consumer
# --bootstrap-server localhost:9092 --topic customer-attributes --from-beginning to see the JSON data
#
# The data will look like this: {"reservationId":"9856743232","amount":"946.88"}
reservationPaymentDF.selectExpr(
    "cast(reservationId as string) as key", "to_json(struct(*)) as value"
).writeStream.format("kafka").option("kafka.bootstrap.servers", "kafka:19092").option(
    "topic", "payment-json"
).option(
    "checkpointLocation", "/tmp/kafkacheckpoint204"
).start().awaitTermination()

# reservationPaymentDF.selectExpr(
#     "cast(reservationId as string) as key", "to_json(struct(*)) as value"
# ).writeStream.outputMode("append").format("console").start().awaitTermination()
