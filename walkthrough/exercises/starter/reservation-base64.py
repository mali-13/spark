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
sparkSession = SparkSession.builder.appName("truck-reservation").getOrCreate()
sparkSession.sparkContext.setLogLevel("WARN")

# Schema for the kafka connect redis source:
# {"key":"dGVzdGtleQ==","existType":"NONE","ch":false,"incr":false,"zSetEntries":[{"element":"dGVzdHZhbHVl","score":0.0}],"zsetEntries":[{"element":"dGVzdHZhbHVl","score":0.0}]}
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

# Create a StructType for the Reservation schema for the following fields: {"reservationId":"814840107",
# "customerName":"Jim Harris", "truckNumber":"15867", "reservationDate":"Sep 29, 2020, 10:06:23 AM"}
reservationSchema = StructType(
    [
        StructField("reservationId", StringType()),
        StructField("customerName", StringType()),
        StructField("truckNumber", StringType()),
        StructField("reservationDate", StringType()),
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

# Using a select expression on the streaming dataframe, cast the key and the value columns from kafka as
# strings, and then select them

# Using the redisMessageSchema StructType, deserialize the JSON from the streaming dataframe

# Create a temporary streaming view called "RedisData" based on the streaming dataframe
# it can later be queried with spark.sql

redisStreamingDF = redisRawStreamingDF.selectExpr(
    "cast(key as string) key", "cast(value as string) value"
)

redisStreamingDF.withColumn("value", from_json("value", redisMessageSchema)).select(
    col("value.*")
).createOrReplaceTempView("RedisData")

# Using spark.sql, select key, zSetEntries[0].element as reservation from RedisData
zSetEntriesEncodedStreamingDF = sparkSession.sql(
    "select key, zSetEntries[0].element as reservation from RedisData"
)

# From the dataframe use the unbase64 function to select a column called reservation with the base64 decoded JSON,
# and cast it to a string
zSetDecodedEntriesStreamingDF = zSetEntriesEncodedStreamingDF.withColumn(
    "reservation", unbase64(zSetEntriesEncodedStreamingDF.reservation).cast("string")
)

# Using the customer location StructType, deserialize the JSON from the streaming dataframe, selecting column
# reservation.* as a temporary view called TruckReservation
zSetDecodedEntriesStreamingDF.withColumn(
    "reservation", from_json("reservation", reservationSchema)
).select(col("reservation.*")).createOrReplaceTempView("TruckReservation")

# Using spark.sql select * from TruckReservation
truckReservationStreamingDF = sparkSession.sql("select * from TruckReservation")

# Write the stream to the console, and configure it to run indefinitely, the console output will look something like
# this:
truckReservationStreamingDF.writeStream.outputMode("append").format(
    "console"
).start().awaitTermination()
